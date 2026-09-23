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
import static com.github.benmanes.caffeine.testing.FutureSubject.assertThat;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;

import com.google.errorprone.annotations.Var;

/**
 * The test cases for the {@link AsyncCache#getWaitingView} waiting views over a shared load.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class AsyncWaitingViewTest {
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final Duration NO_RUSH = Duration.ofMinutes(1);

  private final FakeScheduler scheduler = new FakeScheduler();
  private final AsyncCache<String, String> cache = Caffeine.newBuilder().buildAsync();

  /* --------------- shared load --------------- */

  @Test
  void cancelledView_doesNotAffectOtherWaitersOrLoad() {
    var loaderCalls = new AtomicInteger();
    var loaded = new CompletableFuture<String>();
    BiFunction<String, Executor, CompletableFuture<String>> loader = (key, executor) -> {
      loaderCalls.incrementAndGet();
      return loaded;
    };

    var view1 = cache.getWaitingView(KEY, loader, NO_RUSH, scheduler);
    var view2 = cache.getWaitingView(KEY, loader, NO_RUSH, scheduler);
    assertThat(loaderCalls.get()).isEqualTo(1);

    assertThat(view1.cancel(false)).isTrue();
    assertThat(view1.isCancelled()).isTrue();
    assertThat(loaded).isNotDone();

    loaded.complete(VALUE);
    assertThat(view2.join()).isEqualTo(VALUE);
    assertThat(view1.isCancelled()).isTrue();
    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(cache.getIfPresent(KEY)).succeedsWith(VALUE);
  }

  @Test
  void timedOutView_doesNotAffectOtherWaitersOrLoad() {
    var loaded = new CompletableFuture<String>();
    var view1 = cache.getWaitingView(KEY, (key, executor) -> loaded, Duration.ZERO, scheduler);
    var view2 = cache.getWaitingView(KEY, (key, executor) -> loaded, NO_RUSH, scheduler);

    scheduler.elapse(Duration.ZERO);
    assertThat(view1).failsWith(CompletionException.class)
        .hasCauseThat().isInstanceOf(TimeoutException.class);
    assertThat(loaded).isNotDone();

    loaded.complete(VALUE);
    assertThat(view2.join()).isEqualTo(VALUE);
    assertThat(cache.getIfPresent(KEY)).succeedsWith(VALUE);
  }

  /* --------------- generation binding --------------- */

  @Test
  void staleView_doesNotOverwriteNewValue() {
    var staleLoad = new CompletableFuture<String>();
    var view = cache.getWaitingView(KEY, (key, executor) -> staleLoad, NO_RUSH, scheduler);

    cache.synchronous().invalidate(KEY);
    cache.put(KEY, CompletableFuture.completedFuture("new"));

    staleLoad.complete("old");
    // the old waiter may still receive the old load's result
    assertThat(view.join()).isEqualTo("old");
    // but the view must not write the stale result back over the new value
    assertThat(cache.getIfPresent(KEY)).succeedsWith("new");
  }

  /* --------------- cleanup --------------- */

  @Test
  void completedView_releasesTimeoutTaskAndListeners() {
    var loaded = new CompletableFuture<String>();
    var view1 = cache.getWaitingView(KEY, (key, executor) -> loaded, NO_RUSH, scheduler);
    var view2 = cache.getWaitingView(KEY, (key, executor) -> loaded, NO_RUSH, scheduler);

    var shared = requireNonNull(cache.getIfPresent(KEY));
    assertThat(shared.getNumberOfDependents()).isAtLeast(2);
    assertThat(scheduler.tasks).hasSize(2);

    loaded.complete(VALUE);
    assertThat(view1.join()).isEqualTo(VALUE);
    assertThat(view2.join()).isEqualTo(VALUE);

    assertThat(shared.getNumberOfDependents()).isEqualTo(0);
    assertThat(scheduler.tasks.get(0).isCancelled()).isTrue();
    assertThat(scheduler.tasks.get(1).isCancelled()).isTrue();

    scheduler.elapse();
    assertThat(view1.join()).isEqualTo(VALUE);
    assertThat(view2.join()).isEqualTo(VALUE);
  }

  @Test
  void cancelledView_releasesTimeoutTask() {
    var view = cache.getWaitingView(
        KEY, (key, executor) -> new CompletableFuture<String>(), NO_RUSH, scheduler);
    assertThat(scheduler.tasks).hasSize(1);

    assertThat(view.cancel(false)).isTrue();
    assertThat(scheduler.tasks.get(0).isCancelled()).isTrue();

    scheduler.elapse();
    assertThat(view.isCancelled()).isTrue();
  }

  /* --------------- failure modes --------------- */

  @Test
  void loaderFailure_propagatesToViewAndEvicts() {
    var loaded = new CompletableFuture<String>();
    var view = cache.getWaitingView(KEY, (key, executor) -> loaded, NO_RUSH, scheduler);

    var failure = new IllegalStateException("boom");
    loaded.completeExceptionally(failure);

    assertThat(view).failsWith(CompletionException.class).hasCauseThat().isSameInstanceAs(failure);
    assertThat(cache.getIfPresent(KEY)).isNull();
  }

  @Test
  void loaderThrowsSynchronously_propagatesAndEstablishesNothing() {
    var failure = new RuntimeException("boom");
    BiFunction<String, Executor, CompletableFuture<String>> loader = (key, executor) -> {
      throw failure;
    };

    var thrown = assertThrows(RuntimeException.class,
        () -> cache.getWaitingView(KEY, loader, NO_RUSH, scheduler));
    assertThat(thrown).isSameInstanceAs(failure);
    assertThat(cache.getIfPresent(KEY)).isNull();
    assertThat(scheduler.tasks).isEmpty();
  }

  /* --------------- deadline --------------- */

  @Test
  void mappingFunctionOverload_mergesWithSharedLoad() {
    var view = cache.getWaitingView(KEY, key -> VALUE, NO_RUSH, scheduler);

    assertThat(view).succeedsWith(VALUE);
    assertThat(cache.getIfPresent(KEY)).succeedsWith(VALUE);
    assertThat(scheduler.tasks).isEmpty();
  }

  @Test
  void zeroDeadline_inFlightLoad_timesOut() {
    var loaded = new CompletableFuture<String>();
    var view = cache.getWaitingView(KEY, (key, executor) -> loaded, Duration.ZERO, scheduler);

    assertThat(view.isDone()).isFalse();
    scheduler.elapse();
    assertThat(view).failsWith(CompletionException.class)
        .hasCauseThat().isInstanceOf(TimeoutException.class);

    loaded.complete(VALUE);
    assertThat(cache.getIfPresent(KEY)).succeedsWith(VALUE);
  }

  @Test
  void zeroDeadline_presentValue_returnsImmediately() {
    cache.put(KEY, CompletableFuture.completedFuture(VALUE));

    var view = cache.getWaitingView(KEY, (key, executor) -> {
      throw new AssertionError("the load must be merged, not restarted");
    }, Duration.ZERO, scheduler);

    assertThat(view.join()).isEqualTo(VALUE);
    assertThat(scheduler.tasks).isEmpty();
  }

  /* --------------- races --------------- */

  @Test
  void completionRace_hasSingleOutcome() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      var raceCache = Caffeine.newBuilder().<String, String>buildAsync();
      var raceScheduler = new FakeScheduler();
      var loaded = new CompletableFuture<String>();
      var view = raceCache.getWaitingView(
          KEY, (key, executor) -> loaded, Duration.ofNanos(1), raceScheduler);

      var start = new CountDownLatch(1);
      var completer = new Thread(() -> {
        await(start);
        loaded.complete(VALUE);
      });
      var timer = new Thread(() -> {
        await(start);
        raceScheduler.elapse();
      });
      var canceller = new Thread(() -> {
        await(start);
        view.cancel(false);
      });
      completer.start();
      timer.start();
      canceller.start();
      start.countDown();
      completer.join();
      timer.join();
      canceller.join();

      assertThat(view.isDone()).isTrue();
      if (view.isCancelled()) {
        assertThat(view.isCompletedExceptionally()).isTrue();
      } else if (view.isCompletedExceptionally()) {
        assertThat(view).failsWith(CompletionException.class)
            .hasCauseThat().isInstanceOf(TimeoutException.class);
      } else {
        assertThat(view.join()).isEqualTo(VALUE);
      }
    }
  }

  /* --------------- statistics --------------- */

  @Test
  void stats_viewRecordsNothingBeyondUnderlyingGet() {
    AsyncCache<String, String> statsCache =
        Caffeine.newBuilder().recordStats().buildAsync();
    var loaded = new CompletableFuture<String>();

    var view1 = statsCache.getWaitingView(KEY, (key, executor) -> loaded, NO_RUSH, scheduler);
    var view2 = statsCache.getWaitingView(KEY, (key, executor) -> {
      throw new AssertionError("the load must be merged, not restarted");
    }, NO_RUSH, scheduler);
    var view3 = statsCache.getWaitingView(
        "other", (key, executor) -> new CompletableFuture<String>(), Duration.ZERO, scheduler);

    view1.cancel(false);
    scheduler.elapse(Duration.ZERO);
    loaded.complete(VALUE);
    assertThat(view2.join()).isEqualTo(VALUE);
    assertThat(view3).failsWith(CompletionException.class)
        .hasCauseThat().isInstanceOf(TimeoutException.class);

    var stats = statsCache.synchronous().stats();
    assertThat(stats.missCount()).isEqualTo(2);
    assertThat(stats.hitCount()).isEqualTo(1);
    assertThat(stats.loadSuccessCount()).isEqualTo(1);
    assertThat(stats.loadFailureCount()).isEqualTo(0);
  }

  /* --------------- example --------------- */

  @Test
  @SuppressWarnings("SystemOut")
  void example_printsViewStates() {
    AsyncCache<String, String> exampleCache =
        Caffeine.newBuilder().recordStats().buildAsync();
    var loaderCalls = new AtomicInteger();
    var loaded = new CompletableFuture<String>();
    BiFunction<String, Executor, CompletableFuture<String>> loader = (key, executor) -> {
      loaderCalls.incrementAndGet();
      return loaded;
    };

    var view1 = exampleCache.getWaitingView(
        KEY, loader, Duration.ofSeconds(30), Scheduler.systemScheduler());
    var view2 = exampleCache.getWaitingView(
        KEY, loader, Duration.ofMillis(50), Scheduler.systemScheduler());
    var view3 = exampleCache.getWaitingView(
        KEY, loader, Duration.ofSeconds(30), Scheduler.systemScheduler());
    view3.cancel(false);

    @Var String state2;
    try {
      view2.join();
      state2 = "success";
    } catch (CompletionException e) {
      state2 = requireNonNull(e.getCause()).getClass().getSimpleName();
    }
    loaded.complete(VALUE);

    String state1 = "success(" + view1.join() + ")";
    String state3 = view3.isCancelled() ? "cancelled" : "unexpected";
    String cached = requireNonNull(exampleCache.getIfPresent(KEY)).join();
    System.out.printf("view1=%s view2=%s view3=%s loaderCalls=%d cached=%s%n",
        state1, state2, state3, loaderCalls.get(), cached);

    assertThat(state1).isEqualTo("success(value)");
    assertThat(state2).isEqualTo("TimeoutException");
    assertThat(state3).isEqualTo("cancelled");
    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(cached).isEqualTo(VALUE);
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** A scheduler that queues tasks and runs them only when {@link #elapse} is called. */
  static final class FakeScheduler implements Scheduler {
    final List<FakeTask> tasks = new ArrayList<>();

    @Override
    public Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
      var task = new FakeTask(command, unit.toNanos(delay));
      tasks.add(task);
      return task;
    }

    /** Runs every scheduled task that was not cancelled. */
    void elapse() {
      for (FakeTask task : tasks) {
        task.run();
      }
    }

    /** Runs every scheduled task due within the given delay that was not cancelled. */
    void elapse(Duration delay) {
      for (FakeTask task : tasks) {
        if (task.delayNanos <= delay.toNanos()) {
          task.run();
        }
      }
    }
  }

  /** A scheduled task that runs only on demand and tracks its cancellation. */
  static final class FakeTask implements Future<Object> {
    private final Runnable command;
    private final long delayNanos;
    private boolean ran;
    private boolean cancelled;

    FakeTask(Runnable command, long delayNanos) {
      this.delayNanos = delayNanos;
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
    public Object get() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object get(long timeout, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }
  }
}
