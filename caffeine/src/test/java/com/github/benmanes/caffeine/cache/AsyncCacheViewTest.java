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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.slf4j.event.Level.TRACE;
import static org.slf4j.event.Level.WARN;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import com.google.common.testing.GcFinalization;

/**
 * Tests for {@link AsyncCache#getView}, the isolated waiting view over a shared load.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@CheckMaxLogLevel(TRACE)
final class AsyncCacheViewTest {
  private static final Duration DEADLINE = Duration.ofMinutes(1);

  private final FakeScheduler scheduler = new FakeScheduler();

  private AsyncCache<String, Integer> newCache() {
    return Caffeine.newBuilder().scheduler(scheduler).buildAsync();
  }

  @Test
  void getView_cancelledViewDoesNotAffectOthers() {
    AsyncCache<String, Integer> cache = newCache();
    var loaderCalls = new AtomicInteger();
    var loaderFuture = new CompletableFuture<Integer>();
    BiFunction<String, Executor, CompletableFuture<Integer>> loader = (key, executor) -> {
      loaderCalls.incrementAndGet();
      return loaderFuture;
    };

    var view1 = cache.getView("key", loader, DEADLINE);
    var view2 = cache.getView("key", loader, DEADLINE);

    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(view1).isNotSameInstanceAs(cache.getIfPresent("key"));
    assertThat(view2).isNotSameInstanceAs(view1);

    assertThat(view1.cancel(false)).isTrue();
    assertThat(view1.isCancelled()).isTrue();
    assertThat(view2.isDone()).isFalse();
    assertThat(loaderFuture.isDone()).isFalse();

    loaderFuture.complete(1);

    assertThat(view2.join()).isEqualTo(1);
    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(cache.synchronous().getIfPresent("key")).isEqualTo(1);
    assertThat(scheduler.pendingCount()).isEqualTo(0);
  }

  @Test
  void getView_staleLoadDoesNotOverwriteNewValue() {
    AsyncCache<String, Integer> cache = newCache();
    var loaderFuture = new CompletableFuture<Integer>();
    var view = cache.getView("key", (key, executor) -> loaderFuture, DEADLINE);

    cache.asMap().remove("key");
    cache.put("key", CompletableFuture.completedFuture(2));

    loaderFuture.complete(1);

    assertThat(view.join()).isEqualTo(1);
    assertThat(cache.synchronous().getIfPresent("key")).isEqualTo(2);
  }

  @Test
  void getView_timeout_releasesTaskAndReferences() {
    AsyncCache<String, Integer> cache = newCache();
    var loaderFuture = new CompletableFuture<Integer>();
    @Var var view = cache.getView("key", (key, executor) -> loaderFuture, DEADLINE);
    assertThat(scheduler.pendingCount()).isEqualTo(1);

    var viewRef = new WeakReference<>(view);
    scheduler.runNext();

    assertThat(view.isDone()).isTrue();
    var thrown = assertThrows(CompletionException.class, view::join);
    assertThat(thrown).hasCauseThat().isInstanceOf(TimeoutException.class);
    assertThat(scheduler.pendingCount()).isEqualTo(0);

    // The waiter released the completed view even though the load is still in flight.
    view = null;
    GcFinalization.awaitClear(viewRef);

    // The underlying load continues and populates the cache.
    loaderFuture.complete(1);
    assertThat(cache.synchronous().getIfPresent("key")).isEqualTo(1);
  }

  @Test
  void getView_success_cancelsTimeoutTask() {
    AsyncCache<String, Integer> cache = newCache();
    var loaderFuture = new CompletableFuture<Integer>();
    var view = cache.getView("key", (key, executor) -> loaderFuture, DEADLINE);

    loaderFuture.complete(1);

    assertThat(view.join()).isEqualTo(1);
    assertThat(scheduler.pendingCount()).isEqualTo(0);
    assertThat(scheduler.tasks.getFirst().isCancelled()).isTrue();
  }

  @Test
  @CheckMaxLogLevel(WARN)
  void getView_loadFails() {
    AsyncCache<String, Integer> cache = newCache();
    var loaderFuture = new CompletableFuture<Integer>();
    var view = cache.getView("key", (key, executor) -> loaderFuture, DEADLINE);

    var failure = new IllegalStateException("boom");
    loaderFuture.completeExceptionally(failure);

    var thrown = assertThrows(CompletionException.class, view::join);
    assertThat(thrown).hasCauseThat().isEqualTo(failure);
    assertThat(cache.getIfPresent("key")).isNull();
    assertThat(scheduler.pendingCount()).isEqualTo(0);
  }

  @Test
  void getView_mappingFunctionThrows() {
    AsyncCache<String, Integer> cache = newCache();
    BiFunction<String, Executor, CompletableFuture<Integer>> loader = (key, executor) -> {
      throw new IllegalStateException("boom");
    };

    assertThrows(IllegalStateException.class, () -> cache.getView("key", loader, DEADLINE));
    assertThat(cache.asMap()).isEmpty();
    assertThat(scheduler.pendingCount()).isEqualTo(0);
  }

  @Test
  void getView_zeroDeadline_timesOut() {
    AsyncCache<String, Integer> cache = newCache();
    var view = cache.getView(
        "key", (key, executor) -> new CompletableFuture<Integer>(), Duration.ZERO);
    assertThat(scheduler.pendingCount()).isEqualTo(1);

    scheduler.runNext();

    var thrown = assertThrows(CompletionException.class, view::join);
    assertThat(thrown).hasCauseThat().isInstanceOf(TimeoutException.class);
  }

  @Test
  void getView_zeroDeadline_availableValue() {
    AsyncCache<String, Integer> cache = newCache();
    cache.put("key", CompletableFuture.completedFuture(1));

    var view = cache.getView("key", key -> 2, Duration.ZERO);

    assertThat(view.join()).isEqualTo(1);
    assertThat(scheduler.pendingCount()).isEqualTo(0);
  }

  @Test
  void getView_negativeDeadline() {
    AsyncCache<String, Integer> cache = newCache();
    BiFunction<String, Executor, CompletableFuture<Integer>> loader =
        (key, executor) -> new CompletableFuture<>();
    assertThrows(IllegalArgumentException.class,
        () -> cache.getView("key", loader, Duration.ofSeconds(-1)));
  }

  @Test
  void getView_functionOverload() {
    AsyncCache<String, Integer> cache = Caffeine.newBuilder()
        .scheduler(scheduler)
        .executor(Runnable::run)
        .buildAsync();

    var view = cache.getView("key", key -> 1, DEADLINE);

    assertThat(view.join()).isEqualTo(1);
  }

  @Test
  void getView_completionCancelTimeout_race_resolvesToSingleOutcome() throws Exception {
    for (int i = 0; i < 50; i++) {
      AsyncCache<String, Integer> cache = newCache();
      var loaderFuture = new CompletableFuture<Integer>();
      int value = i;
      var view = cache.getView("key", (key, executor) -> loaderFuture, DEADLINE);

      var ready = new CountDownLatch(1);
      var completer = new Thread(() -> {
        awaitUninterruptibly(ready);
        loaderFuture.complete(value);
      });
      var canceller = new Thread(() -> {
        awaitUninterruptibly(ready);
        view.cancel(false);
      });
      var timer = new Thread(() -> {
        awaitUninterruptibly(ready);
        scheduler.runNext();
      });
      completer.start();
      canceller.start();
      timer.start();
      ready.countDown();
      completer.join();
      canceller.join();
      timer.join();

      assertThat(view.isDone()).isTrue();
      if (!view.isCancelled()) {
        try {
          assertThat(view.join()).isEqualTo(value);
        } catch (CompletionException e) {
          assertThat(e).hasCauseThat().isInstanceOf(TimeoutException.class);
        }
      }
      assertThat(cache.synchronous().getIfPresent("key")).isEqualTo(value);
    }
  }

  @Test
  void getView_recordsStatsLikeGet() {
    AsyncCache<String, Integer> cache = Caffeine.newBuilder()
        .scheduler(scheduler)
        .maximumSize(100)
        .recordStats()
        .buildAsync();
    var loaderFuture = new CompletableFuture<Integer>();
    BiFunction<String, Executor, CompletableFuture<Integer>> loader =
        (key, executor) -> loaderFuture;

    var view1 = cache.getView("key", loader, DEADLINE);
    var view2 = cache.getView("key", loader, DEADLINE);
    view1.cancel(false);
    loaderFuture.complete(1);
    var view3 = cache.getView("key", loader, DEADLINE);

    assertThat(view2.join()).isEqualTo(1);
    assertThat(view3.join()).isEqualTo(1);
    var stats = cache.synchronous().stats();
    assertThat(stats.missCount()).isEqualTo(1);
    assertThat(stats.hitCount()).isEqualTo(2);
    assertThat(stats.loadSuccessCount()).isEqualTo(1);
    assertThat(stats.loadFailureCount()).isEqualTo(0);
  }

  @Test
  void getView_timeout_doesNotRecordLoadFailure() {
    AsyncCache<String, Integer> cache = Caffeine.newBuilder()
        .scheduler(scheduler)
        .recordStats()
        .buildAsync();
    var loaderFuture = new CompletableFuture<Integer>();
    var view = cache.getView("key", (key, executor) -> loaderFuture, DEADLINE);

    scheduler.runNext();
    assertThat(view.isDone()).isTrue();
    loaderFuture.complete(1);

    var stats = cache.synchronous().stats();
    assertThat(stats.missCount()).isEqualTo(1);
    assertThat(stats.loadSuccessCount()).isEqualTo(1);
    assertThat(stats.loadFailureCount()).isEqualTo(0);
  }

  /** An example that prints the view states, loader invocations, and final cached value. */
  @Test
  @SuppressWarnings("SystemOut")
  void example_printsViewStates() {
    AsyncCache<String, Integer> cache = newCache();
    var loaderCalls = new AtomicInteger();
    var loaderFuture = new CompletableFuture<Integer>();
    BiFunction<String, Executor, CompletableFuture<Integer>> loader = (key, executor) -> {
      loaderCalls.incrementAndGet();
      return loaderFuture;
    };

    var viewAlice = cache.getView("key", loader, DEADLINE);
    var viewBob = cache.getView("key", loader, DEADLINE);
    var viewCarol = cache.getView("key", loader, DEADLINE);

    viewBob.cancel(false);
    scheduler.runNext(); // Alice's deadline expires
    loaderFuture.complete(42);

    System.out.printf("viewAlice: %s%n", stateOf(viewAlice));
    System.out.printf("viewBob: %s%n", stateOf(viewBob));
    System.out.printf("viewCarol: %s%n", stateOf(viewCarol));
    System.out.printf("loader invocations: %d%n", loaderCalls.get());
    System.out.printf("final cached value: %s%n", cache.synchronous().getIfPresent("key"));

    var thrown = assertThrows(CompletionException.class, viewAlice::join);
    assertThat(thrown).hasCauseThat().isInstanceOf(TimeoutException.class);
    assertThat(viewBob.isCancelled()).isTrue();
    assertThat(viewCarol.join()).isEqualTo(42);
    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(cache.synchronous().getIfPresent("key")).isEqualTo(42);
  }

  private static String stateOf(CompletableFuture<?> future) {
    if (future.isCancelled()) {
      return "CANCELLED";
    } else if (!future.isDone()) {
      return "WAITING";
    }
    try {
      return "SUCCESS(" + future.join() + ")";
    } catch (CompletionException e) {
      return "FAILED(" + requireNonNull(e.getCause()).getClass().getSimpleName() + ")";
    }
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** A scheduler that captures tasks so that tests can trigger deadlines manually. */
  static final class FakeScheduler implements Scheduler {
    final Deque<FakeTask> tasks = new ArrayDeque<>();

    @Override
    public Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
      var task = new FakeTask(command);
      tasks.addLast(task);
      return task;
    }

    int pendingCount() {
      @Var int count = 0;
      for (var task : tasks) {
        if (!task.isDone()) {
          count++;
        }
      }
      return count;
    }

    /** Runs the next pending task, returning false if there is none. */
    @CanIgnoreReturnValue
    boolean runNext() {
      for (var task : tasks) {
        if (!task.isDone()) {
          task.run();
          return true;
        }
      }
      return false;
    }
  }

  static final class FakeTask implements Future<@Nullable Void> {
    private final Runnable command;
    private boolean ran;
    private boolean cancelled;

    FakeTask(Runnable command) {
      this.command = command;
    }

    synchronized void run() {
      if (!ran && !cancelled) {
        ran = true;
        command.run();
      }
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
      if (ran || cancelled) {
        return false;
      }
      cancelled = true;
      return true;
    }

    @Override
    public synchronized boolean isCancelled() {
      return cancelled;
    }

    @Override
    public synchronized boolean isDone() {
      return ran || cancelled;
    }

    @Override
    public @Nullable Void get() {
      return null;
    }

    @Override
    public @Nullable Void get(long timeout, TimeUnit unit) {
      return null;
    }
  }
}
