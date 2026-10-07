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

import static com.github.benmanes.caffeine.testing.Nullness.nullDuration;
import static com.github.benmanes.caffeine.testing.Nullness.nullRef;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BatchingAsyncCacheLoader}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class BatchingAsyncCacheLoaderTest {
  private final Executor directExecutor = Runnable::run;
  private final List<Set<String>> loads = new ArrayList<>();

  private FakeScheduler scheduler;

  @BeforeEach
  void setUp() {
    scheduler = new FakeScheduler();
    loads.clear();
  }

  @Test
  void coalescesWithinWindow_andSharesResultForDuplicateKeys() {
    var loader = newLoader(this::recordingBulk, /* maxBatchSize= */ 100);

    var firstA = loader.asyncLoad("a", directExecutor);
    var futureB = loader.asyncLoad("b", directExecutor);
    var secondA = loader.asyncLoad("a", directExecutor);

    assertThat(secondA).isSameInstanceAs(firstA);
    assertThat(loads).isEmpty();

    scheduler.runNextTask();

    assertThat(loads).containsExactly(Set.of("a", "b"));
    assertThat(firstA.join()).isEqualTo("A");
    assertThat(futureB.join()).isEqualTo("B");
    assertThat(secondA.join()).isEqualTo("A");
  }

  @Test
  void windowExpires_flushesPendingBatch() {
    var loader = newLoader(this::recordingBulk, /* maxBatchSize= */ 100);

    var future = loader.asyncLoad("a", directExecutor);
    assertThat(future.isDone()).isFalse();
    assertThat(loads).isEmpty();

    scheduler.runNextTask();

    assertThat(loads).containsExactly(Set.of("a"));
    assertThat(future.join()).isEqualTo("A");

    // a subsequent request starts a new batch with its own window
    var next = loader.asyncLoad("c", directExecutor);
    assertThat(loads).hasSize(1);

    scheduler.runNextTask();

    assertThat(loads).containsExactly(Set.of("a"), Set.of("c")).inOrder();
    assertThat(next.join()).isEqualTo("C");
  }

  @Test
  void maxBatchSize_dispatchesImmediately() {
    var loader = newLoader(this::recordingBulk, /* maxBatchSize= */ 2);

    var futureA = loader.asyncLoad("a", directExecutor);
    assertThat(loads).isEmpty();
    assertThat(scheduler.hasPendingTask()).isTrue();

    var futureB = loader.asyncLoad("b", directExecutor);

    // the batch was dispatched without waiting for the window to expire
    assertThat(loads).containsExactly(Set.of("a", "b"));
    assertThat(futureA.join()).isEqualTo("A");
    assertThat(futureB.join()).isEqualTo("B");

    // the window expiration was cancelled, so it does not trigger another load
    scheduler.runNextTask();
    assertThat(loads).hasSize(1);
  }

  @Test
  void asyncLoadAll_coalescesIntoSingleBatch() {
    var loader = newLoader(this::recordingBulk, /* maxBatchSize= */ 100);

    var result = loader.asyncLoadAll(Set.of("a", "b"), directExecutor);
    assertThat(loads).isEmpty();

    scheduler.runNextTask();

    assertThat(loads).containsExactly(Set.of("a", "b"));
    assertThat(result.join()).containsExactly("a", "A", "b", "B");
  }

  @Test
  void partialMissing_completesAbsentKeysWithNull() {
    var loader = newLoader((keys, executor) -> {
      loads.add(Set.copyOf(keys));
      var results = new HashMap<String, String>();
      for (var key : keys) {
        if (!key.equals("b")) {
          results.put(key, key.toUpperCase(Locale.ROOT));
        }
      }
      return CompletableFuture.completedFuture(results);
    }, /* maxBatchSize= */ 100);

    var futureA = loader.asyncLoad("a", directExecutor);
    var futureB = loader.asyncLoad("b", directExecutor);
    scheduler.runNextTask();

    assertThat(loads).containsExactly(Set.of("a", "b"));
    assertThat(futureA.join()).isEqualTo("A");
    assertThat(futureB.isCompletedExceptionally()).isFalse();
    assertThat(futureB.join()).isNull();
  }

  @Test
  void bulkLoadFails_failsAllWaiters() {
    var failure = new RuntimeException("boom");
    var loader = newLoader((keys, executor) -> {
      loads.add(Set.copyOf(keys));
      return CompletableFuture.<Map<String, String>>failedFuture(failure);
    }, /* maxBatchSize= */ 100);

    var futureA = loader.asyncLoad("a", directExecutor);
    var futureB = loader.asyncLoad("b", directExecutor);
    scheduler.runNextTask();

    assertThat(futureA.isCompletedExceptionally()).isTrue();
    assertThat(futureB.isCompletedExceptionally()).isTrue();
    assertThat(assertThrows(ExecutionException.class, futureA::get))
        .hasCauseThat().isSameInstanceAs(failure);
    assertThat(assertThrows(ExecutionException.class, futureB::get))
        .hasCauseThat().isSameInstanceAs(failure);
  }

  @Test
  void bulkLoadThrows_failsAllWaiters() {
    var failure = new RuntimeException("boom");
    var loader = newLoader((keys, executor) -> {
      loads.add(Set.copyOf(keys));
      throw failure;
    }, /* maxBatchSize= */ 100);

    var futureA = loader.asyncLoad("a", directExecutor);
    var futureB = loader.asyncLoad("b", directExecutor);
    scheduler.runNextTask();

    assertThat(assertThrows(ExecutionException.class, futureA::get))
        .hasCauseThat().isSameInstanceAs(failure);
    assertThat(assertThrows(ExecutionException.class, futureB::get))
        .hasCauseThat().isSameInstanceAs(failure);
  }

  @Test
  void cancelPendingRequest_removesKeyFromBatch() {
    var loader = newLoader(this::recordingBulk, /* maxBatchSize= */ 100);

    var futureA = loader.asyncLoad("a", directExecutor);
    var futureB = loader.asyncLoad("b", directExecutor);

    assertThat(futureA.cancel(false)).isTrue();
    assertThat(futureA.isCancelled()).isTrue();

    scheduler.runNextTask();

    assertThat(loads).containsExactly(Set.of("b"));
    assertThat(futureB.join()).isEqualTo("B");
  }

  @Test
  void cancelLastPendingRequest_cancelsWindowAndLoadsNothing() {
    var loader = newLoader(this::recordingBulk, /* maxBatchSize= */ 100);

    var future = loader.asyncLoad("a", directExecutor);
    assertThat(future.cancel(true)).isTrue();

    // the window task was cancelled, so its expiration is a no-op and nothing hangs
    scheduler.runNextTask();

    assertThat(loads).isEmpty();
    assertThat(future.isCancelled()).isTrue();
  }

  @Test
  void shutdown_failsPendingRequestsAndRejectsNewOnes() {
    var loader = newLoader(this::recordingBulk, /* maxBatchSize= */ 100);

    var pending = loader.asyncLoad("a", directExecutor);
    loader.shutdown();

    assertThat(loader.isShutdown()).isTrue();
    assertThat(pending.isDone()).isTrue();
    assertThrows(CancellationException.class, pending::join);

    var rejected = loader.asyncLoad("b", directExecutor);
    assertThat(rejected.isCompletedExceptionally()).isTrue();
    assertThat(assertThrows(ExecutionException.class, rejected::get))
        .hasCauseThat().isInstanceOf(IllegalStateException.class);

    // the window expiration was cancelled; nothing is loaded and nothing hangs
    scheduler.runNextTask();
    assertThat(loads).isEmpty();

    loader.shutdown(); // idempotent
    loader.close();
    assertThat(loader.isShutdown()).isTrue();
  }

  @Test
  void cachedKeys_doNotEnterBatch() {
    var loader = newLoader(this::recordingBulk, /* maxBatchSize= */ 100);
    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);
    cache.put("c", CompletableFuture.completedFuture("C"));

    var result = cache.getAll(List.of("a", "b", "c"));
    assertThat(loads).isEmpty();

    scheduler.runNextTask();

    assertThat(loads).containsExactly(Set.of("a", "b"));
    assertThat(result.join()).containsExactly("a", "A", "b", "B", "c", "C");
  }

  @Test
  void builder_validation() {
    var bulkLoader = AsyncCacheLoader.<String, String>bulk((keys, executor) -> uppercase(keys));
    var builder = BatchingAsyncCacheLoader.<String, String>newBuilder();

    assertThrows(IllegalStateException.class, () -> builder.build(bulkLoader));
    assertThrows(NullPointerException.class, () -> builder.window(nullDuration()));
    assertThrows(NullPointerException.class, () -> builder.scheduler(nullRef()));
    assertThrows(NullPointerException.class, () -> builder.executor(nullRef()));
    assertThrows(NullPointerException.class, () -> builder.build(nullRef()));
    assertThrows(IllegalArgumentException.class, () -> builder.window(Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () -> builder.window(Duration.ofMillis(-1)));
    assertThrows(IllegalArgumentException.class, () -> builder.maxBatchSize(0));
    assertThrows(IllegalArgumentException.class, () -> builder.maxBatchSize(-1));
  }

  /** Returns a loader that records the keys of every bulk load it performs. */
  private BatchingAsyncCacheLoader<String, String> newLoader(
      BiFunction<? super Set<? extends String>, ? super Executor,
          ? extends CompletableFuture<? extends Map<? extends String, ? extends String>>>
          bulkFunction,
      int maxBatchSize) {
    return BatchingAsyncCacheLoader.<String, String>newBuilder()
        .window(Duration.ofMinutes(1))
        .maxBatchSize(maxBatchSize)
        .scheduler(scheduler)
        .executor(directExecutor)
        .build(AsyncCacheLoader.bulk(bulkFunction));
  }

  /** A bulk function that records the requested keys and uppercases them. */
  private CompletableFuture<Map<String, String>> recordingBulk(
      Set<? extends String> keys, Executor executor) {
    loads.add(Set.copyOf(keys));
    return uppercase(keys);
  }

  /** A bulk function that maps each key to its uppercase form. */
  private static CompletableFuture<Map<String, String>> uppercase(Set<? extends String> keys) {
    var results = new LinkedHashMap<String, String>();
    for (var key : keys) {
      results.put(key, key.toUpperCase(Locale.ROOT));
    }
    return CompletableFuture.completedFuture(results);
  }

  /** A scheduler that records tasks and runs them only when explicitly told to. */
  static final class FakeScheduler implements Scheduler {
    private final Deque<ScheduledTask> tasks = new ArrayDeque<>();

    @Override
    public Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
      var task = new ScheduledTask(executor, command);
      tasks.addLast(task);
      return task;
    }

    boolean hasPendingTask() {
      return !tasks.isEmpty();
    }

    void runNextTask() {
      var task = tasks.removeFirst();
      if (!task.isCancelled()) {
        task.executor.execute(task.command);
      }
      task.complete(null);
    }

    static final class ScheduledTask extends CompletableFuture<@Nullable Void> {
      final Executor executor;
      final Runnable command;

      ScheduledTask(Executor executor, Runnable command) {
        this.executor = executor;
        this.command = command;
      }
    }
  }
}
