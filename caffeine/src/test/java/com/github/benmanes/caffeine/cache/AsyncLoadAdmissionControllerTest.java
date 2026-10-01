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

import static com.github.benmanes.caffeine.testing.Nullness.nullRef;
import static com.google.common.truth.Truth.assertThat;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import org.jspecify.annotations.Nullable;

/**
 * Deterministic tests for {@link AsyncLoadAdmissionController}, driven by manual futures.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class AsyncLoadAdmissionControllerTest {
  private static final Executor DIRECT_EXECUTOR = Runnable::run;

  private final ManualLoader loader = new ManualLoader();

  @Test void admission_limitsConcurrencyAndQueue() {
    var controller = AsyncLoadAdmissionController.create(loader, 2, 1);

    var first = controller.asyncLoad("a", DIRECT_EXECUTOR);
    var second = controller.asyncLoad("b", DIRECT_EXECUTOR);
    var third = controller.asyncLoad("c", DIRECT_EXECUTOR);
    var fourth = controller.asyncLoad("d", DIRECT_EXECUTOR);

    assertThat(loader.started).containsExactly("a", "b").inOrder();
    assertThat(controller.runningCount()).isEqualTo(2);
    assertThat(controller.queuedCount()).isEqualTo(1);
    assertThat(first.isDone()).isFalse();
    assertThat(second.isDone()).isFalse();
    assertThat(third.isDone()).isFalse();
    assertRejected(fourth);

    // A request for a running key joins the load without consuming a slot or queue position.
    var repeat = controller.asyncLoad("a", DIRECT_EXECUTOR);
    assertThat(controller.runningCount()).isEqualTo(2);
    assertThat(controller.queuedCount()).isEqualTo(1);
    assertThat(loader.started).containsExactly("a", "b").inOrder();

    var stats = controller.stats();
    assertThat(stats.requestCount()).isEqualTo(5);
    assertThat(stats.coalescedCount()).isEqualTo(1);
    assertThat(stats.startedCount()).isEqualTo(2);
    assertThat(stats.rejectedCount()).isEqualTo(1);

    loader.future("a").complete("va");
    assertThat(first.join()).isEqualTo("va");
    assertThat(repeat.join()).isEqualTo("va");
    assertThat(loader.started).containsExactly("a", "b", "c").inOrder();
  }

  @Test
  @SuppressWarnings({"CheckReturnValue", "FutureReturnValueIgnored"})
  void coalescesQueuedRequests() {
    var controller = AsyncLoadAdmissionController.create(loader, 1, 2);

    controller.asyncLoad("a", DIRECT_EXECUTOR);
    var first = controller.asyncLoad("b", DIRECT_EXECUTOR);
    var second = controller.asyncLoad("b", DIRECT_EXECUTOR);

    assertThat(controller.queuedCount()).isEqualTo(1);
    assertThat(loader.started).containsExactly("a");

    loader.future("a").complete("va");
    assertThat(loader.started).containsExactly("a", "b").inOrder();
    loader.future("b").complete("vb");

    assertThat(first.join()).isEqualTo("vb");
    assertThat(second.join()).isEqualTo("vb");
    assertThat(controller.stats().coalescedCount()).isEqualTo(1);
    assertThat(controller.stats().startedCount()).isEqualTo(2);
  }

  @Test
  @SuppressWarnings({"CheckReturnValue", "FutureReturnValueIgnored"})
  void queueIsFirstInFirstOut() {
    var controller = AsyncLoadAdmissionController.create(loader, 1, 3);

    controller.asyncLoad("a", DIRECT_EXECUTOR);
    controller.asyncLoad("b", DIRECT_EXECUTOR);
    controller.asyncLoad("c", DIRECT_EXECUTOR);
    controller.asyncLoad("d", DIRECT_EXECUTOR);

    for (String key : List.of("a", "b", "c")) {
      loader.future(key).complete("v" + key);
    }
    assertThat(loader.started).containsExactly("a", "b", "c", "d").inOrder();
  }

  @Test void synchronousThrow_releasesSlot() {
    var failure = new IllegalStateException("boom");
    loader.failure = failure;
    var controller = AsyncLoadAdmissionController.create(loader, 1, 1);

    var failed = controller.asyncLoad("a", DIRECT_EXECUTOR);
    loader.failure = null;
    var queued = controller.asyncLoad("b", DIRECT_EXECUTOR);

    assertThat(controller.runningCount()).isEqualTo(1);
    var thrown = assertThrows(ExecutionException.class, failed::get);
    assertThat(thrown).hasCauseThat().isSameInstanceAs(failure);

    // The failed load's slot was released, so the queued load started.
    assertThat(loader.started).containsExactly("a", "b").inOrder();
    loader.future("b").complete("vb");
    assertThat(queued.join()).isEqualTo("vb");

    var stats = controller.stats();
    assertThat(stats.loadFailureCount()).isEqualTo(1);
    assertThat(stats.loadSuccessCount()).isEqualTo(1);
  }

  @Test
  @SuppressWarnings({"CheckReturnValue", "FutureReturnValueIgnored"})
  void lateCompletion_holdsSlotUntilFinished() {
    var controller = AsyncLoadAdmissionController.create(loader, 1, 1);

    controller.asyncLoad("a", DIRECT_EXECUTOR);
    controller.asyncLoad("b", DIRECT_EXECUTOR);

    // The queued load does not start while the running load's future is incomplete.
    assertThat(loader.started).containsExactly("a");
    assertThat(controller.runningCount()).isEqualTo(1);
    assertThat(controller.queuedCount()).isEqualTo(1);

    loader.future("a").complete("va");
    assertThat(loader.started).containsExactly("a", "b").inOrder();
    assertThat(controller.runningCount()).isEqualTo(1);
    assertThat(controller.queuedCount()).isEqualTo(0);
  }

  @Test void executorRejection_releasesSlot() {
    Executor rejectingExecutor = command -> {
      throw new RejectedExecutionException("rejected");
    };
    AsyncCacheLoader<String, String> rejectingLoader = (key, executor) -> {
      executor.execute(() -> {});
      return new CompletableFuture<>();
    };
    var controller = AsyncLoadAdmissionController.create(rejectingLoader, 1, 0);

    var rejected = controller.asyncLoad("a", rejectingExecutor);

    var thrown = assertThrows(ExecutionException.class, rejected::get);
    assertThat(thrown).hasCauseThat().isInstanceOf(RejectedExecutionException.class);
    assertThat(controller.runningCount()).isEqualTo(0);
    assertThat(controller.stats().loadFailureCount()).isEqualTo(1);

    // A rejection by the executor handed to the delegate is not cached; a retry is admitted.
    var retried = controller.asyncLoad("a", DIRECT_EXECUTOR);
    assertThat(controller.runningCount()).isEqualTo(1);
    assertThat(retried.isDone()).isFalse();
  }

  @Test void close_rejectsNewAndQueued_allowsRunningToFinish() {
    var controller = AsyncLoadAdmissionController.create(loader, 1, 1);

    var running = controller.asyncLoad("a", DIRECT_EXECUTOR);
    var queued = controller.asyncLoad("b", DIRECT_EXECUTOR);

    controller.close();

    assertThat(controller.isClosed()).isTrue();
    assertRejected(queued);
    assertRejected(controller.asyncLoad("c", DIRECT_EXECUTOR));
    assertThat(controller.runningCount()).isEqualTo(1);
    assertThat(controller.queuedCount()).isEqualTo(0);

    // The load already running completes normally and delivers its value.
    loader.future("a").complete("va");
    assertThat(running.join()).isEqualTo("va");
    assertThat(controller.runningCount()).isEqualTo(0);

    var stats = controller.stats();
    assertThat(stats.requestCount()).isEqualTo(3);
    assertThat(stats.rejectedCount()).isEqualTo(2);
    assertThat(stats.loadSuccessCount()).isEqualTo(1);
  }

  @Test void cancellationDoesNotCancelSharedLoad() {
    var controller = AsyncLoadAdmissionController.create(loader, 1, 0);

    var first = controller.asyncLoad("a", DIRECT_EXECUTOR);
    var second = controller.asyncLoad("a", DIRECT_EXECUTOR);

    first.cancel(true);

    assertThat(first.isCancelled()).isTrue();
    assertThat(second.isDone()).isFalse();
    assertThat(controller.runningCount()).isEqualTo(1);

    loader.future("a").complete("va");
    assertThat(second.join()).isEqualTo("va");
  }

  @Test void failureIsNotCached_retryStartsNewLoad() {
    var controller = AsyncLoadAdmissionController.create(loader, 1, 0);

    var failed = controller.asyncLoad("a", DIRECT_EXECUTOR);
    loader.future("a").completeExceptionally(new RuntimeException("boom"));
    assertThat(failed.isCompletedExceptionally()).isTrue();
    assertThat(controller.runningCount()).isEqualTo(0);

    var retried = controller.asyncLoad("a", DIRECT_EXECUTOR);
    assertThat(loader.started).containsExactly("a", "a").inOrder();
    loader.future("a").complete("va");
    assertThat(retried.join()).isEqualTo("va");

    var stats = controller.stats();
    assertThat(stats.coalescedCount()).isEqualTo(0);
    assertThat(stats.loadFailureCount()).isEqualTo(1);
    assertThat(stats.loadSuccessCount()).isEqualTo(1);
  }

  @Test
  @SuppressWarnings({"CheckReturnValue", "FutureReturnValueIgnored"})
  void loadTimeExcludesQueueWait() {
    var ticker = new MutableTicker();
    var controller = new AsyncLoadAdmissionController<String, String>(loader, 1, 1, ticker);

    controller.asyncLoad("a", DIRECT_EXECUTOR);
    controller.asyncLoad("b", DIRECT_EXECUTOR);

    ticker.advance(1_000);
    loader.future("a").complete("va");
    ticker.advance(100);
    loader.future("b").complete("vb");

    // The first load ran for 1000ns; the second waited in the queue but ran for only 100ns.
    var stats = controller.stats();
    assertThat(stats.totalLoadTime()).isEqualTo(1_100);
    assertThat(stats.averageLoadTime()).isEqualTo(550.0);
  }

  @Test void cacheHitsBypassAdmission() {
    AsyncCacheLoader<String, String> eagerLoader =
        (key, executor) -> CompletableFuture.completedFuture("v" + key);
    var controller = AsyncLoadAdmissionController.create(eagerLoader, 1, 0);
    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(controller);

    assertThat(cache.get("a").join()).isEqualTo("va");
    assertThat(cache.get("a").join()).isEqualTo("va");

    assertThat(controller.stats().requestCount()).isEqualTo(1);
    assertThat(controller.runningCount()).isEqualTo(0);
  }

  @Test void invalidArguments() {
    assertThrows(IllegalArgumentException.class,
        () -> AsyncLoadAdmissionController.create(loader, 0, 1));
    assertThrows(IllegalArgumentException.class,
        () -> AsyncLoadAdmissionController.create(loader, 1, -1));
    assertThrows(NullPointerException.class,
        () -> AsyncLoadAdmissionController.create(nullRef(), 1, 1));
  }

  private static void assertRejected(CompletableFuture<?> future) {
    var thrown = assertThrows(ExecutionException.class, future::get);
    assertThat(thrown).hasCauseThat().isInstanceOf(LoadRejectedException.class);
  }

  /** A loader whose futures are completed manually by the test. */
  static final class ManualLoader implements AsyncCacheLoader<String, String> {
    final List<String> started = new CopyOnWriteArrayList<>();
    private final Map<String, CompletableFuture<String>> futures = new ConcurrentHashMap<>();
    @Nullable RuntimeException failure;

    CompletableFuture<String> future(String key) {
      return requireNonNull(futures.get(key));
    }

    @Override public CompletableFuture<String> asyncLoad(String key, Executor executor) {
      started.add(key);
      if (failure != null) {
        throw failure;
      }
      var future = new CompletableFuture<String>();
      futures.put(key, future);
      return future;
    }
  }

  /** A ticker advanced manually by the test. */
  static final class MutableTicker implements Ticker {
    private final AtomicLong nanos = new AtomicLong();

    void advance(long delta) {
      nanos.addAndGet(delta);
    }
    @Override public long read() {
      return nanos.get();
    }
  }
}
