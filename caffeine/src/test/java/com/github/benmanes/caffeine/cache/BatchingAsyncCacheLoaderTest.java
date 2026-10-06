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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BatchingAsyncCacheLoader} driven by a {@link FakeScheduler} so that the
 * accumulation window is controlled deterministically.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class BatchingAsyncCacheLoaderTest {
  static final Duration WINDOW = Duration.ofMinutes(1);
  static final Executor DIRECT_EXECUTOR = Runnable::run;

  FakeScheduler scheduler;
  RecordingLoader loader;
  BatchingAsyncCacheLoader<String, String> batching;

  @BeforeEach
  void setUp() {
    scheduler = new FakeScheduler();
    loader = new RecordingLoader();
    batching = BatchingAsyncCacheLoader.batching(loader, WINDOW, 100, scheduler);
  }

  @Test
  void coalescesKeysWithinWindowAndSharesResult() {
    var first = batching.asyncLoad("a", DIRECT_EXECUTOR);
    var second = batching.asyncLoad("b", DIRECT_EXECUTOR);
    var third = batching.asyncLoad("a", DIRECT_EXECUTOR);

    // Nothing is dispatched until the window expires
    assertTrue(loader.batches.isEmpty());
    assertFalse(first.isDone());
    assertFalse(second.isDone());
    assertFalse(third.isDone());

    scheduler.elapse(WINDOW);

    // The loader saw each distinct key exactly once
    assertEquals(List.of(List.of("a", "b")), loader.batches);
    // Every waiter received its own result
    assertEquals("value:a", first.join());
    assertEquals("value:b", second.join());
    assertEquals("value:a", third.join());
  }

  @Test
  void dispatchesWhenWindowExpires() {
    var future = batching.asyncLoad("a", DIRECT_EXECUTOR);

    scheduler.elapse(WINDOW.minusNanos(1));
    assertTrue(loader.batches.isEmpty());
    assertFalse(future.isDone());

    scheduler.elapse(Duration.ofNanos(1));
    assertEquals(List.of(List.of("a")), loader.batches);
    assertEquals("value:a", future.join());
  }

  @Test
  void dispatchesImmediatelyWhenMaxBatchSizeReached() {
    batching = BatchingAsyncCacheLoader.batching(loader, WINDOW, 2, scheduler);

    var first = batching.asyncLoad("a", DIRECT_EXECUTOR);
    assertTrue(loader.batches.isEmpty());

    var second = batching.asyncLoad("b", DIRECT_EXECUTOR);

    // The batch was dispatched without waiting for the window to expire
    assertEquals(List.of(List.of("a", "b")), loader.batches);
    assertEquals("value:a", first.join());
    assertEquals("value:b", second.join());
    // The scheduled window expiration was cancelled
    assertTrue(scheduler.tasks.stream().allMatch(task -> task.cancelled));
  }

  @Test
  void startsNewBatchAfterDispatch() {
    batching = BatchingAsyncCacheLoader.batching(loader, WINDOW, 2, scheduler);

    batching.asyncLoad("a", DIRECT_EXECUTOR);
    batching.asyncLoad("b", DIRECT_EXECUTOR);
    var third = batching.asyncLoad("c", DIRECT_EXECUTOR);

    assertEquals(List.of(List.of("a", "b")), loader.batches);
    assertFalse(third.isDone());

    scheduler.elapse(WINDOW);
    assertEquals(List.of(List.of("a", "b"), List.of("c")), loader.batches);
    assertEquals("value:c", third.join());
  }

  @Test
  void partialMissingKeysCompleteWithNull() {
    loader.missing.add("b");

    var present = batching.asyncLoad("a", DIRECT_EXECUTOR);
    var absent = batching.asyncLoad("b", DIRECT_EXECUTOR);
    scheduler.elapse(WINDOW);

    assertEquals(List.of(List.of("a", "b")), loader.batches);
    assertEquals("value:a", present.join());
    assertTrue(absent.isDone());
    assertFalse(absent.isCompletedExceptionally());
    assertNull(absent.join());
  }

  @Test
  void batchFailureCompletesAllWaitersExceptionally() {
    var failure = new RuntimeException("bulk load failed");
    loader.failure = failure;

    var first = batching.asyncLoad("a", DIRECT_EXECUTOR);
    var second = batching.asyncLoad("b", DIRECT_EXECUTOR);
    scheduler.elapse(WINDOW);

    for (var future : List.of(first, second)) {
      assertTrue(future.isCompletedExceptionally());
      var error = assertThrows(ExecutionException.class, future::get);
      assertSame(failure, error.getCause());
    }
  }

  @Test
  void synchronousLoaderThrowCompletesAllWaitersExceptionally() {
    var failure = new RuntimeException("loader exploded");
    loader.failure = failure;
    loader.throwSynchronously = true;

    var first = batching.asyncLoad("a", DIRECT_EXECUTOR);
    var second = batching.asyncLoad("b", DIRECT_EXECUTOR);
    scheduler.elapse(WINDOW);

    for (var future : List.of(first, second)) {
      assertTrue(future.isCompletedExceptionally());
      var error = assertThrows(ExecutionException.class, future::get);
      assertSame(failure, error.getCause());
    }
  }

  @Test
  void cancelledWaiterIsRemovedFromPendingBatch() {
    var cancelled = batching.asyncLoad("a", DIRECT_EXECUTOR);
    var kept = batching.asyncLoad("b", DIRECT_EXECUTOR);

    assertTrue(cancelled.cancel(true));
    assertTrue(cancelled.isCancelled());

    scheduler.elapse(WINDOW);

    // The cancelled key never reached the loader and nobody is left hanging
    assertEquals(List.of(List.of("b")), loader.batches);
    assertEquals("value:b", kept.join());
  }

  @Test
  void cancelledWaiterDoesNotAffectOtherWaitersOfSameKey() {
    var cancelled = batching.asyncLoad("a", DIRECT_EXECUTOR);
    var kept = batching.asyncLoad("a", DIRECT_EXECUTOR);

    assertTrue(cancelled.cancel(true));
    scheduler.elapse(WINDOW);

    assertEquals(List.of(List.of("a")), loader.batches);
    assertEquals("value:a", kept.join());
  }

  @Test
  void cancellingLastWaiterCancelsScheduledWindow() {
    var future = batching.asyncLoad("a", DIRECT_EXECUTOR);
    assertTrue(future.cancel(true));

    assertTrue(scheduler.tasks.stream().allMatch(task -> task.cancelled));
    scheduler.elapse(WINDOW);
    assertTrue(loader.batches.isEmpty());
  }

  @Test
  void shutdownFlushesPendingBatch() {
    var pending = batching.asyncLoad("a", DIRECT_EXECUTOR);

    batching.shutdown();

    assertTrue(batching.isShutdown());
    assertEquals(List.of(List.of("a")), loader.batches);
    assertEquals("value:a", pending.join());
  }

  @Test
  void loadsAfterShutdownBypassBatching() {
    batching.shutdown();

    var future = batching.asyncLoad("c", DIRECT_EXECUTOR);

    assertEquals("value:c", future.join());
    assertEquals(List.of("c"), loader.singleLoads);
    assertTrue(loader.batches.isEmpty());
  }

  @Test
  void shutdownIsIdempotent() {
    batching.shutdown();
    batching.shutdown();
    assertTrue(batching.isShutdown());
    assertTrue(loader.batches.isEmpty());
  }

  @Test
  void cacheHitsDoNotEnterBatch() {
    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(batching);

    var first = cache.get("a");
    var second = cache.get("b");
    scheduler.elapse(WINDOW);
    assertEquals("value:a", first.join());
    assertEquals("value:b", second.join());
    assertEquals(1, loader.batches.size());

    // A cached key is served from the cache and never enters another batch
    assertEquals("value:a", cache.get("a").join());
    assertEquals(1, loader.batches.size());
    assertEquals(1, loader.batchRequests.get());
  }

  @Test
  void concurrentLoadsNeverHang() throws Exception {
    batching = BatchingAsyncCacheLoader.batching(
        loader, Duration.ofMillis(5), 25, Scheduler.systemScheduler());
    int threads = 8;
    int loadsPerThread = 250;
    var futures = new ArrayList<Map.Entry<String, CompletableFuture<String>>>();
    var workers = new ArrayList<Thread>();
    for (int i = 0; i < threads; i++) {
      int threadId = i;
      var worker = new Thread(() -> {
        for (int j = 0; j < loadsPerThread; j++) {
          String key = "key-" + ((threadId + j) % 64);
          var future = batching.asyncLoad(key, DIRECT_EXECUTOR);
          synchronized (futures) {
            futures.add(Map.entry(key, future));
          }
        }
      });
      workers.add(worker);
      worker.start();
    }
    for (var worker : workers) {
      worker.join();
    }
    batching.shutdown();
    // No future is left hanging: every waiter completes with its own value
    assertEquals(threads * loadsPerThread, futures.size());
    for (var request : futures) {
      assertEquals("value:" + request.getKey(),
          request.getValue().get(1, TimeUnit.MINUTES));
    }
  }

  @Test
  void invalidArguments() {
    assertThrows(NullPointerException.class,
        () -> BatchingAsyncCacheLoader.batching(null, WINDOW, 1, scheduler));
    assertThrows(NullPointerException.class,
        () -> BatchingAsyncCacheLoader.batching(loader, null, 1, scheduler));
    assertThrows(NullPointerException.class,
        () -> BatchingAsyncCacheLoader.batching(loader, WINDOW, 1, null));
    assertThrows(IllegalArgumentException.class,
        () -> BatchingAsyncCacheLoader.batching(loader, Duration.ofNanos(-1), 1, scheduler));
    assertThrows(IllegalArgumentException.class,
        () -> BatchingAsyncCacheLoader.batching(loader, WINDOW, 0, scheduler));
  }

  /** A scheduler that fires tasks only when the test advances its clock. */
  static final class FakeScheduler implements Scheduler {
    final List<Task> tasks = new ArrayList<>();
    long nowNanos;

    @Override
    public Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
      var task = new Task(executor, command, nowNanos + unit.toNanos(delay));
      tasks.add(task);
      return task;
    }

    void elapse(Duration duration) {
      nowNanos += duration.toNanos();
      for (var i = tasks.iterator(); i.hasNext();) {
        var task = i.next();
        if (!task.cancelled && !task.done && (task.fireAtNanos <= nowNanos)) {
          i.remove();
          task.done = true;
          task.executor.execute(task.command);
        }
      }
    }

    static final class Task implements Future<Object> {
      final Executor executor;
      final Runnable command;
      final long fireAtNanos;
      boolean cancelled;
      boolean done;

      Task(Executor executor, Runnable command, long fireAtNanos) {
        this.executor = requireNonNull(executor);
        this.command = requireNonNull(command);
        this.fireAtNanos = fireAtNanos;
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        if (done || cancelled) {
          return false;
        }
        cancelled = true;
        return true;
      }

      @Override
      public boolean isCancelled() {
        return cancelled;
      }

      @Override
      public boolean isDone() {
        return done || cancelled;
      }

      @Override
      public Object get() throws ExecutionException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object get(long timeout, TimeUnit unit)
          throws ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
      }
    }
  }

  /** A loader that records the keys it was asked to load and how it responded. */
  static final class RecordingLoader implements AsyncCacheLoader<String, String> {
    final List<List<String>> batches = new ArrayList<>();
    final List<String> singleLoads = new ArrayList<>();
    final Set<String> missing = new HashSet<>();
    final AtomicInteger batchRequests = new AtomicInteger();
    RuntimeException failure;
    boolean throwSynchronously;

    @Override
    public CompletableFuture<String> asyncLoad(String key, Executor executor) {
      singleLoads.add(key);
      return CompletableFuture.completedFuture("value:" + key);
    }

    @Override
    public CompletableFuture<Map<String, String>> asyncLoadAll(
        Set<? extends String> keys, Executor executor) {
      batchRequests.incrementAndGet();
      batches.add(List.copyOf(keys));
      if (failure != null) {
        if (throwSynchronously) {
          throw failure;
        }
        return CompletableFuture.failedFuture(failure);
      }
      var result = new HashMap<String, String>();
      for (var key : keys) {
        if (!missing.contains(key)) {
          result.put(key, "value:" + key);
        }
      }
      return CompletableFuture.completedFuture(result);
    }
  }
}
