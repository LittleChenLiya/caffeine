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

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * An {@link AsyncCacheLoader} decorator that coalesces individual load requests into short bulk
 * loads. Some backends can fetch many keys in a single call much more efficiently than serving
 * many small requests; this loader accumulates the keys requested within a configurable time
 * window and dispatches them as one {@link AsyncCacheLoader#asyncLoadAll} call.
 * <p>
 * A batch is dispatched when either the window since the first pending key elapses, or the number
 * of pending keys reaches the configured maximum batch size, whichever happens first. A key that
 * is requested multiple times while a batch is being accumulated is added only once, and all of
 * the callers waiting for that key share the same loaded value. Keys that are already present in
 * the cache never reach this loader and therefore never enter a batch.
 * <p>
 * If the bulk load completes exceptionally, or the underlying loader throws, then every waiter of
 * the batch is completed exceptionally. If the bulk result does not contain an entry for a
 * requested key, then the waiters of that key are completed with a {@code null} value (mirroring
 * the partial-result semantics of {@link AsyncLoadingCache#getAll}). Cancelling the future
 * returned to a caller removes the key from a not-yet-dispatched batch when no other caller is
 * waiting for it, so a cancelled request neither blocks nor pollutes the batch.
 * <p>
 * Usage example:
 * <pre>{@code
 *   AsyncCacheLoader<K, V> batching = BatchingAsyncCacheLoader.batching(
 *       bulkLoader, Duration.ofMillis(10), 100, scheduler);
 *   AsyncLoadingCache<K, V> cache = Caffeine.newBuilder().buildAsync(batching);
 * }</pre>
 * <p>
 * This class is thread-safe and may be shared by multiple caches.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NullMarked
public final class BatchingAsyncCacheLoader<K, V extends @Nullable Object>
    implements AsyncCacheLoader<K, V> {

  final AsyncCacheLoader<K, V> loader;
  final long windowNanos;
  final int maxBatchSize;
  final Scheduler scheduler;
  final Object lock = new Object();

  @GuardedBy("lock")
  final Map<K, BatchEntry<K, V>> pending = new LinkedHashMap<>();
  @GuardedBy("lock")
  @Nullable Future<?> scheduled;
  @GuardedBy("lock")
  @Nullable Executor windowExecutor;
  @GuardedBy("lock")
  boolean shutdown;

  BatchingAsyncCacheLoader(
      AsyncCacheLoader<K, V> loader, Duration window, int maxBatchSize, Scheduler scheduler) {
    this.loader = requireNonNull(loader);
    this.scheduler = requireNonNull(scheduler);
    this.windowNanos = requireNonNull(window).toNanos();
    if (window.isNegative()) {
      throw new IllegalArgumentException("window must not be negative: " + window);
    }
    if (maxBatchSize < 1) {
      throw new IllegalArgumentException("maxBatchSize must be positive: " + maxBatchSize);
    }
    this.maxBatchSize = maxBatchSize;
  }

  /**
   * Returns an {@link AsyncCacheLoader} that batches individual load requests into bulk loads.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @param loader the loader whose {@link AsyncCacheLoader#asyncLoadAll} performs the bulk load
   * @param window how long to accumulate keys before dispatching a batch
   * @param maxBatchSize the maximum number of distinct keys to accumulate before dispatching a
   *        batch immediately
   * @param scheduler the scheduler used to fire the window expiration
   * @return a loader that coalesces individual loads into bulk loads
   * @throws NullPointerException if any argument is null
   * @throws IllegalArgumentException if {@code window} is negative or {@code maxBatchSize} is
   *         not positive
   */
  public static <K, V extends @Nullable Object> BatchingAsyncCacheLoader<K, V> batching(
      AsyncCacheLoader<K, V> loader, Duration window, int maxBatchSize, Scheduler scheduler) {
    return new BatchingAsyncCacheLoader<>(loader, window, maxBatchSize, scheduler);
  }

  @Override
  public CompletableFuture<V> asyncLoad(K key, Executor executor) {
    requireNonNull(key);
    requireNonNull(executor);

    BatchEntry<K, V> entry;
    Map<K, BatchEntry<K, V>> batch = null;
    Executor dispatchExecutor = executor;
    synchronized (lock) {
      if (shutdown) {
        entry = null;
      } else {
        entry = pending.computeIfAbsent(key, k -> new BatchEntry<>());
        entry.waiters++;
        if (pending.size() == 1) {
          windowExecutor = executor;
          try {
            scheduled = scheduler.schedule(
                executor, this::flush, windowNanos, TimeUnit.NANOSECONDS);
          } catch (RuntimeException e) {
            // The scheduler rejected the task; dispatch immediately so waiters never hang
            batch = drainLocked();
          }
        }
        if ((batch == null) && (pending.size() >= maxBatchSize)) {
          batch = drainLocked();
        }
        if (batch != null) {
          dispatchExecutor = windowExecutor;
        }
      }
    }
    if (entry == null) {
      return loadDirectly(key, executor);
    }
    var waiter = new WaiterFuture(key, entry);
    entry.shared.whenComplete((value, error) -> {
      if (error == null) {
        waiter.complete(value);
      } else {
        waiter.completeExceptionally(error);
      }
    });
    if (batch != null) {
      dispatch(batch, dispatchExecutor);
    }
    return waiter;
  }

  @Override
  public CompletableFuture<? extends Map<? extends K, ? extends V>> asyncLoadAll(
      Set<? extends K> keys, Executor executor) throws Exception {
    // Bulk requests are already batched, so they bypass the accumulation window
    return loader.asyncLoadAll(keys, executor);
  }

  /**
   * Stops accumulating new batches and dispatches the currently pending keys, if any, so that no
   * waiter is left hanging. Loads requested after this method has been called are delegated
   * directly to the underlying loader, unbatched.
   */
  public void shutdown() {
    Map<K, BatchEntry<K, V>> batch;
    Executor executor;
    synchronized (lock) {
      if (shutdown) {
        return;
      }
      shutdown = true;
      batch = drainLocked();
      executor = (windowExecutor != null) ? windowExecutor : Runnable::run;
    }
    if (!batch.isEmpty()) {
      dispatch(batch, executor);
    }
  }

  /** Returns whether {@link #shutdown} has been called. */
  public boolean isShutdown() {
    synchronized (lock) {
      return shutdown;
    }
  }

  /** Dispatches the pending batch when the accumulation window expires. */
  void flush() {
    Map<K, BatchEntry<K, V>> batch;
    Executor executor;
    synchronized (lock) {
      scheduled = null;
      if (pending.isEmpty()) {
        return;
      }
      batch = drainLocked();
      executor = windowExecutor;
    }
    dispatch(batch, executor);
  }

  /** Removes and returns the pending keys, cancelling any scheduled window expiration. */
  @GuardedBy("lock")
  Map<K, BatchEntry<K, V>> drainLocked() {
    cancelScheduledLocked();
    var batch = new LinkedHashMap<K, BatchEntry<K, V>>(pending);
    pending.clear();
    return batch;
  }

  @GuardedBy("lock")
  void cancelScheduledLocked() {
    if (scheduled != null) {
      scheduled.cancel(/* mayInterruptIfRunning= */ false);
      scheduled = null;
    }
  }

  /** Performs a bulk load and completes the waiters of each key in the batch. */
  void dispatch(Map<K, BatchEntry<K, V>> batch, Executor executor) {
    CompletableFuture<? extends Map<? extends K, ? extends V>> future;
    try {
      future = loader.asyncLoadAll(new LinkedHashSet<>(batch.keySet()), executor);
    } catch (Throwable t) {
      completeAll(batch, /* result= */ null, t);
      return;
    }
    if (future == null) {
      completeAll(batch, /* result= */ null,
          new NullPointerException("Bulk loader returned a null future"));
      return;
    }
    future.whenComplete((result, error) -> completeAll(batch, result, error));
  }

  /** Completes every entry in the batch with either the loaded value or the failure. */
  static <K, V extends @Nullable Object> void completeAll(Map<K, BatchEntry<K, V>> batch,
      @Nullable Map<? extends K, ? extends V> result, @Nullable Throwable error) {
    if (error != null) {
      for (var entry : batch.values()) {
        entry.shared.completeExceptionally(error);
      }
    } else {
      batch.forEach((key, entry) ->
          entry.shared.complete((result == null) ? null : result.get(key)));
    }
  }

  /** Releases the interest of a waiter in a key, dropping it from a pending batch if unobserved. */
  void release(K key, BatchEntry<K, V> entry) {
    synchronized (lock) {
      if ((--entry.waiters == 0) && pending.remove(key, entry) && pending.isEmpty()) {
        cancelScheduledLocked();
      }
    }
  }

  /** Loads a single key directly, bypassing the batch after {@link #shutdown}. */
  CompletableFuture<V> loadDirectly(K key, Executor executor) {
    try {
      @SuppressWarnings("unchecked")
      var future = (CompletableFuture<V>) loader.asyncLoad(key, executor);
      return requireNonNull(future, "Loader returned a null future");
    } catch (Throwable t) {
      var future = new CompletableFuture<V>();
      future.completeExceptionally(t);
      return future;
    }
  }

  /** The shared result for a key being accumulated into a batch. */
  static final class BatchEntry<K, V extends @Nullable Object> {
    final CompletableFuture<V> shared = new CompletableFuture<>();
    int waiters;
  }

  /** A per-caller view of a batched result that can be cancelled independently. */
  final class WaiterFuture extends CompletableFuture<V> {
    final K key;
    final BatchEntry<K, V> entry;

    WaiterFuture(K key, BatchEntry<K, V> entry) {
      this.key = key;
      this.entry = entry;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        release(key, entry);
      }
      return cancelled;
    }
  }
}
