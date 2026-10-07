/*
 * Copyright 2026 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache;

import static java.util.Objects.requireNonNull;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * An {@link AsyncCacheLoader} that coalesces load requests arriving within a short time window
 * into a single bulk load, for use with backends that are more efficient when loading many keys
 * at once. This strategy assumes that the efficiency gained through batching justifies the small
 * additional latency of waiting for the window to fill or expire.
 * <p>
 * A batch is dispatched to the bulk loader when either the configured window elapses (measured
 * from the first request of the batch) or the number of distinct pending keys reaches the
 * configured maximum batch size, whichever happens first. Requests for the same key that arrive
 * while the key is still pending are attached to the same batch and share a single result
 * future, so the bulk loader sees each key at most once per batch. When this loader is supplied
 * to {@link Caffeine#buildAsync}, keys that are already cached are served by the cache and never
 * enter a batch.
 * <p>
 * The window is driven by a user-supplied {@link Scheduler}, which allows tests to use a fake
 * scheduler for deterministic behavior. Cancelling a pending request removes it from its batch;
 * shutting the loader down fails all pending requests and rejects new ones, so no future
 * returned by this loader is left permanently incomplete.
 * <p>
 * Usage example:
 * <pre>{@code
 *   AsyncLoadingCache<String, User> cache = Caffeine.newBuilder()
 *       .buildAsync(BatchingAsyncCacheLoader.<String, User>newBuilder()
 *           .window(Duration.ofMillis(10))
 *           .maxBatchSize(100)
 *           .build(AsyncCacheLoader.bulk((keys, executor) -> loadUsers(keys))));
 * }</pre>
 *
 * @param <K> the type of keys
 * @param <V> the type of values. A loader may return null values if and only if it declares a
 *     nullable value type. (Null values are still never <i>stored</i> in the cache.)
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NullMarked
public final class BatchingAsyncCacheLoader<K, V extends @Nullable Object>
    implements AsyncCacheLoader<K, V>, AutoCloseable {
  private static final Logger logger =
      System.getLogger(BatchingAsyncCacheLoader.class.getName());

  private final AsyncCacheLoader<K, V> bulkLoader;
  private final Scheduler scheduler;
  private final Executor executor;
  private final long windowNanos;
  private final int maxBatchSize;

  private final Object lock = new Object();
  @GuardedBy("lock")
  private final Map<K, PendingFuture> pending = new LinkedHashMap<>();
  @GuardedBy("lock")
  private @Nullable Future<?> windowFuture;
  @GuardedBy("lock")
  private boolean shutdown;

  private BatchingAsyncCacheLoader(Builder<K, V> builder, AsyncCacheLoader<K, V> bulkLoader) {
    this.bulkLoader = requireNonNull(bulkLoader);
    this.scheduler = builder.scheduler;
    this.executor = builder.executor;
    this.windowNanos = builder.windowNanos;
    this.maxBatchSize = builder.maxBatchSize;
  }

  /**
   * Returns a builder that constructs a {@link BatchingAsyncCacheLoader} with the window set by
   * {@link Builder#window} and the batch size set by {@link Builder#maxBatchSize}.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new builder
   */
  public static <K, V extends @Nullable Object> Builder<K, V> newBuilder() {
    return new Builder<>();
  }

  /**
   * {@inheritDoc}
   * <p>
   * The key is added to the current batch and the returned future completes when the batch is
   * loaded. If the key is already pending, the existing shared future is returned. The supplied
   * {@code executor} is ignored in favor of the executor set on the builder.
   */
  @Override
  public CompletableFuture<V> asyncLoad(K key, Executor executor) {
    requireNonNull(key);
    synchronized (lock) {
      if (shutdown) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("The loader has been shut down"));
      }
      @Var var pendingFuture = pending.get(key);
      if (pendingFuture != null) {
        return pendingFuture;
      }
      pendingFuture = new PendingFuture(key);
      pending.put(key, pendingFuture);
      if (pending.size() >= maxBatchSize) {
        dispatchLocked();
      } else if (pending.size() == 1) {
        scheduleWindowLocked();
      }
      return pendingFuture;
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Each key is routed through {@link #asyncLoad} so that it joins the current batch; the
   * returned future completes when all of the individual results are available. Keys whose
   * loaded value is null are omitted from the result map.
   */
  @Override
  public CompletableFuture<Map<K, @NonNull V>> asyncLoadAll(
      Set<? extends K> keys, Executor executor) {
    requireNonNull(keys);
    var futures = new LinkedHashMap<K, CompletableFuture<V>>(keys.size());
    for (K key : keys) {
      futures.put(key, asyncLoad(key, executor));
    }
    return CompletableFuture
        .allOf(futures.values().toArray(CompletableFuture<?>[]::new))
        .thenApply(ignored -> {
          var results = new LinkedHashMap<K, @NonNull V>(futures.size());
          futures.forEach((key, future) -> {
            V value = future.join();
            if (value != null) {
              results.put(key, value);
            }
          });
          return results;
        });
  }

  /** Flushes the pending batch when the window expires. */
  private void flush() {
    Map<K, PendingFuture> batch;
    synchronized (lock) {
      batch = drainLocked();
    }
    if (!batch.isEmpty()) {
      load(batch);
    }
  }

  /** Dispatches the pending batch, if any, for loading. */
  @GuardedBy("lock")
  private void dispatchLocked() {
    var batch = drainLocked();
    if (batch.isEmpty()) {
      return;
    }
    try {
      executor.execute(() -> load(batch));
    } catch (Throwable t) {
      logger.log(Level.WARNING, "Exception thrown when submitting batch load; running inline", t);
      load(batch);
    }
  }

  /** Schedules the window expiration for the current batch. */
  @GuardedBy("lock")
  private void scheduleWindowLocked() {
    try {
      windowFuture = scheduler.schedule(executor, this::flush, windowNanos, TimeUnit.NANOSECONDS);
    } catch (Throwable t) {
      logger.log(Level.WARNING, "Exception thrown by scheduler; dispatching batch immediately", t);
      dispatchLocked();
    }
  }

  /** Removes and returns the pending batch, cancelling the window expiration if scheduled. */
  @GuardedBy("lock")
  private Map<K, PendingFuture> drainLocked() {
    cancelWindowLocked();
    if (pending.isEmpty()) {
      return Map.of();
    }
    var batch = new LinkedHashMap<>(pending);
    pending.clear();
    return batch;
  }

  /** Cancels the window expiration if it was scheduled. */
  @GuardedBy("lock")
  private void cancelWindowLocked() {
    if (windowFuture != null) {
      windowFuture.cancel(/* mayInterruptIfRunning= */ false);
      windowFuture = null;
    }
  }

  /** Performs the bulk load and completes the futures of the waiting requests. */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void load(Map<K, PendingFuture> batch) {
    CompletableFuture<? extends Map<? extends K, ? extends @NonNull V>> result;
    try {
      result = bulkLoader.asyncLoadAll(new LinkedHashSet<>(batch.keySet()), executor);
    } catch (Throwable t) {
      batch.values().forEach(future -> future.completeExceptionally(t));
      return;
    }
    if (result == null) {
      var failure = new NullPointerException("The bulk loader returned a null future");
      batch.values().forEach(future -> future.completeExceptionally(failure));
      return;
    }
    result.whenComplete((results, error) -> {
      if (results == null) {
        var failure = (error == null)
            ? new NullPointerException("The bulk loader returned null results")
            : error;
        batch.values().forEach(future -> future.completeExceptionally(failure));
      } else {
        batch.forEach((key, future) -> complete(future, results.get(key)));
      }
    });
  }

  /** Completes the {@code future} with the loaded {@code value}, or null if the key is absent. */
  @SuppressWarnings("NullAway") // a key missing from the results completes with a null value
  private void complete(PendingFuture future, @Nullable V value) {
    future.complete(value);
  }

  /**
   * Signals that no further requests should be accepted. All pending requests are completed
   * exceptionally with a {@link CancellationException} and subsequent requests fail immediately
   * with an {@link IllegalStateException}. This method is idempotent.
   */
  public void shutdown() {
    Map<K, PendingFuture> batch;
    synchronized (lock) {
      if (shutdown) {
        return;
      }
      shutdown = true;
      batch = drainLocked();
    }
    var failure = new CancellationException("The loader has been shut down");
    batch.values().forEach(future -> future.completeExceptionally(failure));
  }

  /** Returns whether this loader has been shut down. */
  public boolean isShutdown() {
    synchronized (lock) {
      return shutdown;
    }
  }

  /** Shuts down this loader, as described by {@link #shutdown()}. */
  @Override
  public void close() {
    shutdown();
  }

  /** A future that removes itself from the pending batch when cancelled. */
  final class PendingFuture extends CompletableFuture<V> {
    final K key;

    PendingFuture(K key) {
      this.key = key;
    }

    @Override public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        synchronized (lock) {
          if (pending.remove(key, this) && pending.isEmpty()) {
            cancelWindowLocked();
          }
        }
      }
      return cancelled;
    }
  }

  /** A builder of {@link BatchingAsyncCacheLoader} instances. */
  @NullMarked
  public static final class Builder<K, V extends @Nullable Object> {
    private static final int DEFAULT_MAX_BATCH_SIZE = 100;

    private Scheduler scheduler = Scheduler.systemScheduler();
    private Executor executor = ForkJoinPool.commonPool();
    private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    private long windowNanos = -1L;

    private Builder() {}

    /**
     * Sets the maximum time to wait for requests to accumulate before the batch is loaded. The
     * window starts when the first request of a batch arrives. This setting is required.
     *
     * @param window the duration of the batching window
     * @return this builder instance (for chaining)
     * @throws NullPointerException if {@code window} is null
     * @throws IllegalArgumentException if {@code window} is not positive
     */
    @CanIgnoreReturnValue
    public Builder<K, V> window(Duration window) {
      requireNonNull(window);
      if (window.isNegative() || window.isZero()) {
        throw new IllegalArgumentException("window must be positive");
      }
      windowNanos = window.toNanos();
      return this;
    }

    /**
     * Sets the maximum number of distinct keys to accumulate before the batch is loaded
     * immediately, without waiting for the window to expire. If not set, defaults to
     * {@value #DEFAULT_MAX_BATCH_SIZE}.
     *
     * @param maxBatchSize the maximum number of distinct keys per batch
     * @return this builder instance (for chaining)
     * @throws IllegalArgumentException if {@code maxBatchSize} is not positive
     */
    @CanIgnoreReturnValue
    public Builder<K, V> maxBatchSize(int maxBatchSize) {
      if (maxBatchSize <= 0) {
        throw new IllegalArgumentException("maxBatchSize must be positive");
      }
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    /**
     * Sets the scheduler used to expire the batching window. If not set, defaults to
     * {@link Scheduler#systemScheduler()}.
     *
     * @param scheduler the scheduler that triggers the window expiration
     * @return this builder instance (for chaining)
     * @throws NullPointerException if {@code scheduler} is null
     */
    @CanIgnoreReturnValue
    public Builder<K, V> scheduler(Scheduler scheduler) {
      this.scheduler = requireNonNull(scheduler);
      return this;
    }

    /**
     * Sets the executor on which bulk loads are performed. If not set, defaults to the
     * {@link ForkJoinPool#commonPool()}.
     *
     * @param executor the executor that performs bulk loads
     * @return this builder instance (for chaining)
     * @throws NullPointerException if {@code executor} is null
     */
    @CanIgnoreReturnValue
    public Builder<K, V> executor(Executor executor) {
      this.executor = requireNonNull(executor);
      return this;
    }

    /**
     * Returns a loader that batches the individual lookups to the system of record. The supplied
     * loader must implement {@link AsyncCacheLoader#asyncLoadAll}, e.g. by way of
     * {@link AsyncCacheLoader#bulk}.
     *
     * @param bulkLoader the loader that computes the values for a batch of keys
     * @return a loader that coalesces individual lookups into bulk loads
     * @throws NullPointerException if {@code bulkLoader} is null
     * @throws IllegalStateException if the window was not set
     */
    public BatchingAsyncCacheLoader<K, V> build(AsyncCacheLoader<K, V> bulkLoader) {
      requireNonNull(bulkLoader);
      if (windowNanos <= 0L) {
        throw new IllegalStateException("window must be set");
      }
      return new BatchingAsyncCacheLoader<>(this, bulkLoader);
    }
  }
}
