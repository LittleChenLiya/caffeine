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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import com.google.errorprone.annotations.Var;

/**
 * An {@link AsyncCacheLoader} adapter that bounds the admission of loads to a delegate loader.
 * The adapter permits at most {@code maxConcurrency} loads to run against the delegate at a time
 * and holds at most {@code maxQueueSize} additional loads in a FIFO waiting queue. A request that
 * cannot be admitted is rejected with a future failed by {@link LoadRejectedException}; the
 * failure is not cached, so a later request may be admitted and retried.
 * <p>
 * Requests for a key whose load is already in flight, whether running or queued, join that load
 * and receive a dependent future without consuming an additional slot or queue position. Each
 * requester receives an independent future, so cancelling one caller's wait does not cancel the
 * shared load or affect other waiters.
 * <p>
 * A concurrency slot is held from the moment the delegate loader is invoked until its future
 * completes, it throws, or the executor rejects the work; the slot is then released and the next
 * queued load, if any, is started. Load timing statistics measure only the time a load holds a
 * slot, never the time spent waiting in the queue. The decision of whether a dequeued load may
 * start is made atomically with respect to {@link #close}: once closed, new requests and queued
 * loads are rejected, while loads already running are allowed to finish.
 * <p>
 * Cache hits never reach this adapter, as the cache invokes its loader only on a miss, so hits
 * consume no slot or queue capacity. Refreshing an entry through {@link #asyncReload} is admitted
 * on the same terms as a load. Bulk loading is not supported; {@link AsyncLoadingCache#getAll}
 * falls back to individual admitted loads.
 * <p>
 * Usage example:
 * <pre>{@code
 * AsyncCacheLoader<String, String> loader = (key, executor) ->
 *     CompletableFuture.supplyAsync(() -> fetch(key), executor);
 * var admission = AsyncLoadAdmissionController.create(loader, 2, 8);
 * AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(admission);
 * }</pre>
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NullMarked
public final class AsyncLoadAdmissionController<K, V extends @Nullable Object>
    implements AsyncCacheLoader<K, V>, AutoCloseable {
  private final AsyncCacheLoader<K, V> delegate;
  private final int maxConcurrency;
  private final int maxQueueSize;
  private final Ticker ticker;

  private final Object lock = new Object();
  private final Map<K, InFlight<K, V>> inFlightByKey = new HashMap<>();
  private final ArrayDeque<InFlight<K, V>> queue = new ArrayDeque<>();
  private int running;
  private boolean closed;

  private long requestCount;
  private long coalescedCount;
  private long startedCount;
  private long rejectedCount;
  private long loadSuccessCount;
  private long loadFailureCount;
  private long totalLoadTime;

  AsyncLoadAdmissionController(AsyncCacheLoader<K, V> delegate, int maxConcurrency,
      int maxQueueSize, Ticker ticker) {
    this.delegate = requireNonNull(delegate);
    this.ticker = requireNonNull(ticker);
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("maxConcurrency must be positive");
    }
    if (maxQueueSize < 0) {
      throw new IllegalArgumentException("maxQueueSize must not be negative");
    }
    this.maxConcurrency = maxConcurrency;
    this.maxQueueSize = maxQueueSize;
  }

  /**
   * Returns an adapter that admits at most {@code maxConcurrency} concurrent loads to the
   * {@code delegate} loader and queues at most {@code maxQueueSize} additional loads.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @param delegate the loader to admit loads to
   * @param maxConcurrency the maximum number of loads running against the delegate at a time
   * @param maxQueueSize the maximum number of loads waiting for a concurrency slot
   * @return an admission-controlling adapter over the delegate loader
   * @throws IllegalArgumentException if {@code maxConcurrency} is not positive or
   *     {@code maxQueueSize} is negative
   */
  public static <K, V extends @Nullable Object> AsyncLoadAdmissionController<K, V> create(
      AsyncCacheLoader<K, V> delegate, int maxConcurrency, int maxQueueSize) {
    return new AsyncLoadAdmissionController<>(
        delegate, maxConcurrency, maxQueueSize, Ticker.systemTicker());
  }

  /**
   * Admits a load for the {@code key}, joining any in-flight load of the same key. The executor
   * of the request that established the in-flight load is the one handed to the delegate.
   *
   * @return a future that completes with the load's result, or fails with
   *     {@link LoadRejectedException} if the load was not admitted
   */
  @Override public CompletableFuture<V> asyncLoad(K key, Executor executor) {
    requireNonNull(key);
    requireNonNull(executor);

    InFlight<K, V> inFlight;
    boolean startNow;
    synchronized (lock) {
      requestCount++;
      if (closed) {
        rejectedCount++;
        return rejectedFuture("the admission controller is closed");
      }
      InFlight<K, V> existing = inFlightByKey.get(key);
      if (existing != null) {
        coalescedCount++;
        return existing.subscribe();
      }
      inFlight = new InFlight<>(key, executor);
      if (running < maxConcurrency) {
        admitLocked(inFlight);
        startNow = true;
      } else if (queue.size() < maxQueueSize) {
        queue.addLast(inFlight);
        startNow = false;
      } else {
        rejectedCount++;
        return rejectedFuture("the admission queue is full");
      }
      inFlightByKey.put(key, inFlight);
    }
    if (startNow) {
      start(inFlight);
    }
    return inFlight.subscribe();
  }

  /** Returns the number of loads currently holding a concurrency slot. */
  public int runningCount() {
    synchronized (lock) {
      return running;
    }
  }

  /** Returns the number of loads currently waiting in the queue. */
  public int queuedCount() {
    synchronized (lock) {
      return queue.size();
    }
  }

  /** Returns whether this controller has been closed. */
  public boolean isClosed() {
    synchronized (lock) {
      return closed;
    }
  }

  /** Returns a snapshot of the admission statistics. */
  public LoadAdmissionStats stats() {
    synchronized (lock) {
      return new LoadAdmissionStats(requestCount, coalescedCount, startedCount, rejectedCount,
          loadSuccessCount, loadFailureCount, totalLoadTime);
    }
  }

  /**
   * Rejects new requests and queued loads, and allows loads already running to finish. The
   * delegate loader and the executors handed to it are not shut down.
   */
  @Override public void close() {
    List<InFlight<K, V>> rejected;
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
      rejected = new ArrayList<>(queue);
      queue.clear();
      for (InFlight<K, V> inFlight : rejected) {
        inFlight.done = true;
        inFlightByKey.remove(inFlight.key, inFlight);
        rejectedCount++;
      }
    }
    for (InFlight<K, V> inFlight : rejected) {
      inFlight.promise.completeExceptionally(
          new LoadRejectedException("the admission controller is closed"));
    }
  }

  /** Reserves a concurrency slot for the load; the caller starts it outside the lock. */
  private void admitLocked(InFlight<K, V> inFlight) {
    inFlight.startTimeNanos = ticker.read();
    running++;
    startedCount++;
  }

  /** Invokes the delegate loader and wires its completion to release the slot. */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void start(InFlight<K, V> inFlight) {
    CompletableFuture<? extends V> future;
    try {
      future = delegate.asyncLoad(inFlight.key, inFlight.executor);
    } catch (Throwable t) {
      finish(inFlight, null, t);
      return;
    }
    if (future == null) {
      finish(inFlight, null, new NullPointerException("The delegate loader returned null"));
      return;
    }
    future.whenComplete((value, error) -> finish(inFlight, value, error));
  }

  /** Releases the load's slot, records its outcome, and starts or rejects the next queued load. */
  @SuppressWarnings({"DataFlowIssue", "NullAway"})
  private void finish(InFlight<K, V> inFlight, @Nullable V value, @Nullable Throwable error) {
    @Var InFlight<K, V> next = null;
    synchronized (lock) {
      if (inFlight.done) {
        return;
      }
      inFlight.done = true;
      inFlightByKey.remove(inFlight.key, inFlight);
      running--;
      totalLoadTime += ticker.read() - inFlight.startTimeNanos;
      if (error == null) {
        loadSuccessCount++;
      } else {
        loadFailureCount++;
      }
      if (!closed) {
        next = queue.pollFirst();
        if (next != null) {
          admitLocked(next);
        }
      }
    }
    if (error == null) {
      inFlight.promise.complete(value);
    } else {
      inFlight.promise.completeExceptionally(error);
    }
    if (next != null) {
      start(next);
    }
  }

  private CompletableFuture<V> rejectedFuture(String reason) {
    return CompletableFuture.failedFuture(new LoadRejectedException(reason));
  }

  /** A load shared by every requester of its key, whether running or waiting in the queue. */
  private static final class InFlight<K, V extends @Nullable Object> {
    final CompletableFuture<V> promise = new CompletableFuture<>();
    final K key;
    final Executor executor;
    boolean done;
    long startTimeNanos;

    InFlight(K key, Executor executor) {
      this.key = key;
      this.executor = executor;
    }

    /** Returns a dependent future so that cancelling one waiter does not cancel the load. */
    @SuppressWarnings({"DataFlowIssue", "FutureReturnValueIgnored", "NullAway"})
    CompletableFuture<V> subscribe() {
      var listener = new CompletableFuture<V>();
      promise.whenComplete((value, error) -> {
        if (error == null) {
          listener.complete(value);
        } else {
          listener.completeExceptionally(error);
        }
      });
      return listener;
    }
  }
}
