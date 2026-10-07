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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.google.common.testing.FakeTicker;

/**
 * Tests for {@link FailureMemoizingCache}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class FailureMemoizingCacheTest {
  static final Duration TTL = Duration.ofMinutes(1);

  final FakeTicker ticker = new FakeTicker();

  static final class TestException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    TestException(String message) {
      super(message);
    }
  }

  static final class OtherException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    OtherException(String message) {
      super(message);
    }
  }

  /** A loader whose futures are completed manually by the test. */
  static final class ManualLoader implements AsyncCacheLoader<String, String> {
    final List<CompletableFuture<String>> futures = new ArrayList<>();

    @Override
    public CompletableFuture<String> asyncLoad(String key, Executor executor) {
      var future = new CompletableFuture<String>();
      futures.add(future);
      return future;
    }

    int calls() {
      return futures.size();
    }
  }

  private FailureMemoizingCache.Builder<String, String> newBuilder() {
    return FailureMemoizingCache.<String, String>newBuilder()
        .memoizable(TestException.class::isInstance)
        .failureTtl(TTL)
        .maximumFailures(16)
        .ticker(ticker::read)
        .executor(Runnable::run);
  }

  @Test
  void memoizedFailureServedUntilTtlExpires() {
    var calls = new AtomicInteger();
    var failure = new TestException("boom");
    var cache = newBuilder()
        .loader(key -> {
          if (calls.incrementAndGet() == 1) {
            throw failure;
          }
          return "value";
        })
        .build();

    // the first load fails and the failure is memoized
    var first = assertThrows(TestException.class, () -> cache.get("a"));
    assertThat(first).isSameInstanceAs(failure);
    assertThat(calls.get()).isEqualTo(1);
    assertThat(cache.memoizedFailures()).isEqualTo(1);

    // within the TTL the failure is served without invoking the loader, with a visible cause
    var memoized = assertThrows(MemoizedFailureException.class, () -> cache.get("a"));
    assertThat(memoized).hasCauseThat().isSameInstanceAs(failure);
    ticker.advance(TTL.minusNanos(1));
    assertThrows(MemoizedFailureException.class, () -> cache.get("a"));
    assertThat(calls.get()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(2);

    // after the TTL expires the loader is invoked again and the success is cached
    ticker.advance(Duration.ofNanos(1));
    assertThat(cache.get("a")).isEqualTo("value");
    assertThat(cache.get("a")).isEqualTo("value");
    assertThat(calls.get()).isEqualTo(2);
    assertThat(cache.failureHits()).isEqualTo(2);
    assertThat(cache.memoizedFailures()).isEqualTo(1);
  }

  @Test
  void staleFailureCannotMaskSuccessAfterInvalidate() {
    var loader = new ManualLoader();
    var cache = newBuilder().asyncLoader(loader).build();

    CompletableFuture<String> first = cache.getAsync("a");
    assertThat(loader.calls()).isEqualTo(1);

    // invalidate while the first load is in flight, then reload successfully
    cache.invalidate("a");
    CompletableFuture<String> second = cache.getAsync("a");
    assertThat(loader.calls()).isEqualTo(2);
    loader.futures.get(1).complete("new");
    assertThat(second.join()).isEqualTo("new");

    // the stale failure completes after the invalidation and must not mask the success
    loader.futures.get(0).completeExceptionally(new TestException("stale"));
    assertThat(first.isCompletedExceptionally()).isTrue();
    assertThat(cache.get("a")).isEqualTo("new");
    assertThat(cache.memoizedFailures()).isEqualTo(0);
    assertThat(cache.failureHits()).isEqualTo(0);
    assertThat(loader.calls()).isEqualTo(2);
  }

  @Test
  void invalidateClearsMemoizedFailure() {
    var loader = new ManualLoader();
    var failure = new TestException("boom");
    var cache = newBuilder().asyncLoader(loader).build();

    CompletableFuture<String> first = cache.getAsync("a");
    loader.futures.get(0).completeExceptionally(failure);
    assertThat(first.isCompletedExceptionally()).isTrue();

    // the memoized failure is served without loading, with a visible cause
    var memoized = assertThrows(MemoizedFailureException.class, () -> cache.get("a"));
    assertThat(memoized).hasCauseThat().isSameInstanceAs(failure);
    assertThat(cache.failureHits()).isEqualTo(1);
    assertThat(loader.calls()).isEqualTo(1);

    // an explicit invalidate clears both the failure record and the cached value
    cache.invalidate("a");
    CompletableFuture<String> second = cache.getAsync("a");
    assertThat(loader.calls()).isEqualTo(2);
    loader.futures.get(1).complete("value");
    assertThat(second.join()).isEqualTo("value");
    assertThat(cache.get("a")).isEqualTo("value");
    assertThat(cache.memoizedFailures()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(1);
  }

  @Test
  void invalidateAllClearsFailuresAndStaleRecording() {
    var loader = new ManualLoader();
    var cache = newBuilder().asyncLoader(loader).build();

    // memoize a failure for one key
    CompletableFuture<String> failed = cache.getAsync("b");
    loader.futures.get(0).completeExceptionally(new TestException("boom"));
    assertThat(failed.isCompletedExceptionally()).isTrue();
    assertThrows(MemoizedFailureException.class, () -> cache.get("b"));
    assertThat(cache.failureHits()).isEqualTo(1);

    // start a load for another key, then invalidate everything
    CompletableFuture<String> pending = cache.getAsync("a");
    cache.invalidateAll();

    // the memoized failure record was cleared, so the key is loaded again
    CompletableFuture<String> reloaded = cache.getAsync("b");
    assertThat(loader.calls()).isEqualTo(3);

    // a stale failure completing after invalidateAll must not be recorded
    loader.futures.get(1).completeExceptionally(new TestException("stale"));
    assertThat(pending.isCompletedExceptionally()).isTrue();
    loader.futures.get(2).complete("value-b");
    assertThat(reloaded.join()).isEqualTo("value-b");
    assertThat(cache.memoizedFailures()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(1);
  }

  @Test
  void nonMemoizableFailureIsNotRecorded() {
    var calls = new AtomicInteger();
    var cache = newBuilder()
        .loader(key -> {
          calls.incrementAndGet();
          throw new OtherException("fatal");
        })
        .build();

    assertThrows(OtherException.class, () -> cache.get("a"));
    assertThrows(OtherException.class, () -> cache.get("a"));
    assertThat(calls.get()).isEqualTo(2);
    assertThat(cache.memoizedFailures()).isEqualTo(0);
    assertThat(cache.failureHits()).isEqualTo(0);
  }

  @Test
  void failureRecordsAreCapacityBounded() {
    var calls = new AtomicInteger();
    var cache = FailureMemoizingCache.<String, String>newBuilder()
        .loader(key -> {
          calls.incrementAndGet();
          throw new TestException("boom");
        })
        .memoizable(TestException.class::isInstance)
        .failureTtl(Duration.ofHours(1))
        .maximumFailures(1)
        .ticker(ticker::read)
        .executor(Runnable::run)
        .build();

    // k1's failure is memoized and served without reloading
    assertThrows(TestException.class, () -> cache.get("k1"));
    assertThrows(MemoizedFailureException.class, () -> cache.get("k1"));
    assertThat(cache.failureHits()).isEqualTo(1);

    // k2's failure competes for the single failure slot
    assertThrows(TestException.class, () -> cache.get("k2"));
    assertThat(calls.get()).isEqualTo(2);

    // at most one failure can be memoized, so at least one key must reload; the counts must
    // remain consistent regardless of which record was evicted
    int callsBefore = calls.get();
    long hitsBefore = cache.failureHits();
    assertThrows(RuntimeException.class, () -> cache.get("k1"));
    assertThrows(RuntimeException.class, () -> cache.get("k2"));
    int reloads = calls.get() - callsBefore;
    long hits = cache.failureHits() - hitsBefore;
    assertThat(reloads).isAtLeast(1);
    assertThat(hits + reloads).isEqualTo(2);
    assertThat(cache.memoizedFailures()).isEqualTo(calls.get());
  }

  @Test
  void concurrentLoadersShareSingleFailure() throws Exception {
    int threads = 8;
    var calls = new AtomicInteger();
    var shared = new CompletableFuture<String>();
    var failure = new TestException("boom");
    var cache = newBuilder()
        .asyncLoader((key, executor) -> {
          calls.incrementAndGet();
          return shared;
        })
        .build();

    var issued = new CountDownLatch(threads);
    @SuppressWarnings({"rawtypes", "unchecked"})
    CompletableFuture<String>[] results = new CompletableFuture[threads];
    Thread[] workers = new Thread[threads];
    for (int i = 0; i < threads; i++) {
      int index = i;
      workers[i] = new Thread(() -> {
        results[index] = cache.getAsync("a");
        issued.countDown();
      });
      workers[i].start();
    }
    assertThat(issued.await(10, TimeUnit.SECONDS)).isTrue();
    shared.completeExceptionally(failure);
    for (int i = 0; i < threads; i++) {
      workers[i].join(10_000);
      CompletableFuture<String> result = results[i];
      var thrown = assertThrows(CompletionException.class, result::join);
      assertThat(thrown).hasCauseThat().isSameInstanceAs(failure);
    }

    // concurrent loads for the same key were coalesced into one load and one memoized failure
    assertThat(calls.get()).isEqualTo(1);
    assertThat(cache.memoizedFailures()).isEqualTo(1);
    assertThat(cache.failureHits()).isEqualTo(0);

    // the next request is served the memoized failure without invoking the loader
    var memoized = assertThrows(MemoizedFailureException.class, () -> cache.get("a"));
    assertThat(memoized).hasCauseThat().isSameInstanceAs(failure);
    assertThat(cache.failureHits()).isEqualTo(1);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void customSuccessCacheIsUsed() {
    var calls = new AtomicInteger();
    var cache = FailureMemoizingCache.<String, String>newBuilder()
        .loader(key -> "value-" + calls.incrementAndGet())
        .memoizable(TestException.class::isInstance)
        .failureTtl(TTL)
        .successCache(Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .ticker(ticker::read))
        .ticker(ticker::read)
        .executor(Runnable::run)
        .build();

    // successes are cached by the custom success cache
    assertThat(cache.get("a")).isEqualTo("value-1");
    assertThat(cache.get("a")).isEqualTo("value-1");
    assertThat(calls.get()).isEqualTo(1);

    // and expire according to its own configuration
    ticker.advance(Duration.ofMinutes(6));
    assertThat(cache.get("a")).isEqualTo("value-2");
    assertThat(calls.get()).isEqualTo(2);
  }
}
