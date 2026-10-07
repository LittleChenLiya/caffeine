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

import static com.github.benmanes.caffeine.testing.Awaits.await;
import static com.github.benmanes.caffeine.testing.ConcurrentTestHarness.executor;
import static com.github.benmanes.caffeine.testing.ConcurrentTestHarness.scheduledExecutor;
import static com.github.benmanes.caffeine.testing.ConcurrentTestHarness.timeTasks;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.google.common.testing.FakeTicker;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Tests for {@link FailureMemoizingLoadingCache}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class FailureMemoizingLoadingCacheTest {
  private static final Duration TTL = Duration.ofMinutes(1);
  private static final int MAX_FAILURES = 10;

  private final FakeTicker ticker = new FakeTicker();
  private final AtomicInteger loads = new AtomicInteger();

  /** A loader exception that the test caches are configured to memoize. */
  private static final class MemoizedException extends RuntimeException {
    private static final long serialVersionUID = 1;

    MemoizedException(String message) {
      super(message);
    }
  }

  private FailureMemoizingLoadingCache<String, String> newCache(CacheLoader<String, String> loader) {
    return newCache(loader, MemoizedException.class::isInstance, MAX_FAILURES);
  }

  private FailureMemoizingLoadingCache<String, String> newCache(
      CacheLoader<String, String> loader, Predicate<? super Throwable> memorizable,
      long maximumFailures) {
    LoadingCache<String, String> delegate = Caffeine.newBuilder().build(loader);
    return FailureMemoizingLoadingCache.memoizeFailures(
        delegate, memorizable, TTL, maximumFailures, ticker::read);
  }

  @Test void memoizedFailureRethrownUntilTtlExpires() {
    var fail = new AtomicBoolean(true);
    var cache = newCache(key -> {
      loads.incrementAndGet();
      if (fail.get()) {
        throw new MemoizedException("unavailable");
      }
      return "value-" + key;
    });

    var first = assertThrows(MemoizedException.class, () -> cache.get("a"));
    var second = assertThrows(MemoizedException.class, () -> cache.get("a"));
    assertThat(second).isSameInstanceAs(first);
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(1);
    assertThat(cache.memoizedFailureCount()).isEqualTo(1);

    ticker.advance(TTL.minusNanos(1));
    assertThrows(MemoizedException.class, () -> cache.get("a"));
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(2);

    ticker.advance(Duration.ofNanos(2));
    fail.set(false);
    assertThat(cache.get("a")).isEqualTo("value-a");
    assertThat(loads.get()).isEqualTo(2);
    assertThat(cache.failureHits()).isEqualTo(2);
  }

  @Test void memoizedFailurePreservesCause() {
    var cache = newCache(new CacheLoader<String, String>() {
      @Override public String load(String key) throws IOException {
        loads.incrementAndGet();
        throw new IOException("unavailable");
      }
    }, CompletionException.class::isInstance, MAX_FAILURES);

    var first = assertThrows(CompletionException.class, () -> cache.get("a"));
    assertThat(first).hasCauseThat().isInstanceOf(IOException.class);

    var second = assertThrows(CompletionException.class, () -> cache.get("a"));
    assertThat(second).isSameInstanceAs(first);
    assertThat(second).hasCauseThat().isSameInstanceAs(first.getCause());
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(1);
  }

  @Test void nonMemorizedFailureIsNotRecorded() {
    var cache = newCache(key -> {
      loads.incrementAndGet();
      throw new IllegalArgumentException("not memorizable");
    });

    assertThrows(IllegalArgumentException.class, () -> cache.get("a"));
    assertThrows(IllegalArgumentException.class, () -> cache.get("a"));
    assertThat(loads.get()).isEqualTo(2);
    assertThat(cache.failureHits()).isEqualTo(0);
    assertThat(cache.memoizedFailureCount()).isEqualTo(0);
  }

  @Test void invalidateClearsMemoizedFailure() {
    var fail = new AtomicBoolean(true);
    var cache = newCache(key -> {
      loads.incrementAndGet();
      if (fail.get()) {
        throw new MemoizedException("unavailable");
      }
      return "value-" + key;
    });

    assertThrows(MemoizedException.class, () -> cache.get("a"));
    fail.set(false);

    cache.invalidate("a");
    assertThat(cache.get("a")).isEqualTo("value-a");
    assertThat(loads.get()).isEqualTo(2);
    assertThat(cache.failureHits()).isEqualTo(0);
    assertThat(cache.memoizedFailureCount()).isEqualTo(0);
  }

  @Test void invalidateAllClearsMemoizedFailures() {
    var fail = new AtomicBoolean(true);
    var cache = newCache(key -> {
      loads.incrementAndGet();
      if (fail.get()) {
        throw new MemoizedException("unavailable");
      }
      return "value-" + key;
    });

    assertThrows(MemoizedException.class, () -> cache.get("a"));
    assertThrows(MemoizedException.class, () -> cache.get("b"));
    fail.set(false);

    cache.invalidateAll();
    assertThat(cache.get("a")).isEqualTo("value-a");
    assertThat(cache.get("b")).isEqualTo("value-b");
    assertThat(loads.get()).isEqualTo(4);
    assertThat(cache.failureHits()).isEqualTo(0);
    assertThat(cache.memoizedFailureCount()).isEqualTo(0);
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored") // the release is a one-shot signal
  void staleFailureDoesNotMaskSuccess() throws Exception {
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var cache = newCache(key -> {
      if (loads.incrementAndGet() == 1) {
        started.countDown();
        Uninterruptibles.awaitUninterruptibly(release);
        throw new MemoizedException("stale");
      }
      return "value-" + key;
    });

    // A load that will fail is in flight; the invalidation is ordered before it completes.
    var blocked = executor.submit(
        () -> assertThrows(MemoizedException.class, () -> cache.get("a")));
    started.await();
    scheduledExecutor.schedule(release::countDown, 100, TimeUnit.MILLISECONDS);
    cache.invalidate("a");

    assertThat(cache.get("a")).isEqualTo("value-a");
    blocked.get();

    // The stale failure completed after the invalidation and must not shadow the new value.
    assertThat(cache.get("a")).isEqualTo("value-a");
    assertThat(loads.get()).isEqualTo(2);
    assertThat(cache.memoizedFailureCount()).isEqualTo(0);
    assertThat(cache.failureHits()).isEqualTo(0);
  }

  @Test void capacityEvictionForgetsFailures() {
    var cache = newCache(key -> {
      loads.incrementAndGet();
      throw new MemoizedException("unavailable");
    }, MemoizedException.class::isInstance, /* maximumFailures= */ 2);
    var keys = List.of("a", "b", "c", "d", "e");

    for (var key : keys) {
      assertThrows(MemoizedException.class, () -> cache.get(key));
    }
    cache.cleanUp();
    assertThat(cache.memoizedFailureCount()).isAtMost(2);
    assertThat(cache.failureHits()).isEqualTo(0);

    // The retained failures are served without loading; the evicted ones load again. Every
    // rejected call is accounted for: it either hit a memoized failure or invoked the loader.
    for (var key : keys) {
      assertThrows(MemoizedException.class, () -> cache.get(key));
    }
    assertThat(loads.get()).isGreaterThan(keys.size());
    assertThat(loads.get() + cache.failureHits()).isEqualTo(2 * keys.size());
  }

  @Test void concurrentMemoizedHits() {
    var cache = newCache(key -> {
      loads.incrementAndGet();
      throw new MemoizedException("unavailable");
    });
    assertThrows(MemoizedException.class, () -> cache.get("a"));
    assertThat(loads.get()).isEqualTo(1);

    int threads = 8;
    timeTasks(threads, () -> assertThrows(MemoizedException.class, () -> cache.get("a")));
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(threads);
    assertThat(cache.memoizedFailureCount()).isEqualTo(1);
  }

  @Test void putSupersedesMemoizedFailure() {
    var cache = newCache(key -> {
      loads.incrementAndGet();
      throw new MemoizedException("unavailable");
    });
    assertThrows(MemoizedException.class, () -> cache.get("a"));

    cache.put("a", "manual");
    assertThat(cache.get("a")).isEqualTo("manual");
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(0);
    assertThat(cache.memoizedFailureCount()).isEqualTo(0);
  }

  @Test void successfulRefreshClearsMemoizedFailure() {
    var fail = new AtomicBoolean(true);
    var cache = newCache(key -> {
      loads.incrementAndGet();
      if (fail.get()) {
        throw new MemoizedException("unavailable");
      }
      return "value-" + key;
    });
    assertThrows(MemoizedException.class, () -> cache.get("a"));

    fail.set(false);
    cache.refresh("a").join();
    // the refreshed value is installed by a callback that may run after the future completes
    await().until(() -> cache.getIfPresent("a") != null);
    assertThat(cache.get("a")).isEqualTo("value-a");
    assertThat(loads.get()).isEqualTo(2);
    assertThat(cache.memoizedFailureCount()).isEqualTo(0);
  }

  @Test void getAllRethrowsMemoizedFailure() {
    var cache = newCache(key -> {
      loads.incrementAndGet();
      if (key.equals("a")) {
        throw new MemoizedException("unavailable");
      }
      return "value-" + key;
    });
    assertThrows(MemoizedException.class, () -> cache.get("a"));

    assertThrows(MemoizedException.class, () -> cache.getAll(List.of("a", "b")));
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(1);
  }
}
