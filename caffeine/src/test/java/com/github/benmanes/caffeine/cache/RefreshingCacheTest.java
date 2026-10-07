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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.testing.ConcurrentTestHarness;
import com.google.common.testing.FakeTicker;

/**
 * Unit tests for {@link RefreshingCache} driven by a fake ticker and manually completed loader
 * futures, so that the fresh, stale, and expired states are exercised deterministically.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class RefreshingCacheTest {
  private static final String KEY = "key";
  private static final Duration FRESH = Duration.ofMinutes(1);
  private static final Duration STALE = Duration.ofMinutes(1);

  private final FakeTicker ticker = new FakeTicker();
  private final ManualLoader loader = new ManualLoader();
  private final RefreshingCache<String, String> cache = RefreshingCache.newBuilder()
      .freshFor(FRESH)
      .staleFor(STALE)
      .ticker(ticker::read)
      .build(loader);

  /** Loads the initial value so that the entry is fresh at the current ticker time. */
  private void prime(String value) {
    var future = cache.get(KEY);
    assertThat(future.isDone()).isFalse();
    loader.lastFuture().complete(value);
    assertThat(future.join()).isEqualTo(value);
  }

  @Test
  void get_absent_waitsForLoad() {
    var future = cache.get(KEY);
    assertThat(future.isDone()).isFalse();
    assertThat(loader.loads.get()).isEqualTo(1);

    loader.lastFuture().complete("v1");
    assertThat(future.join()).isEqualTo("v1");
    assertThat(cache.getIfPresent(KEY)).isEqualTo("v1");
  }

  @Test
  void get_freshHit_servesValueWithoutLoading() {
    prime("v1");
    assertThat(loader.loads.get()).isEqualTo(1);

    ticker.advance(Duration.ofSeconds(30));
    var fresh = cache.get(KEY);
    assertThat(fresh.isDone()).isTrue();
    assertThat(fresh.join()).isEqualTo("v1");
    assertThat(loader.loads.get()).isEqualTo(1);
  }

  @Test
  void get_staleHit_servesOldValueAndRefreshesOnce() {
    prime("v1");

    ticker.advance(Duration.ofSeconds(90)); // past the fresh period, within the staleness cap
    var stale = cache.get(KEY);
    assertThat(stale.isDone()).isTrue();
    assertThat(stale.join()).isEqualTo("v1");
    assertThat(loader.loads.get()).isEqualTo(2);

    // a repeated read while the refresh is in flight reuses it and still serves the old value
    var again = cache.get(KEY);
    assertThat(again.isDone()).isTrue();
    assertThat(again.join()).isEqualTo("v1");
    assertThat(loader.loads.get()).isEqualTo(2);

    loader.lastFuture().complete("v2");
    assertThat(cache.get(KEY).join()).isEqualTo("v2");
    assertThat(loader.loads.get()).isEqualTo(2);
  }

  @Test
  void get_beyondStalenessCap_waitsForSharedLoad() {
    prime("v1");

    ticker.advance(Duration.ofSeconds(150)); // past fresh + stale, the old value may not be served
    var waiting = cache.get(KEY);
    assertThat(waiting.isDone()).isFalse();
    assertThat(loader.loads.get()).isEqualTo(2);

    var concurrent = cache.get(KEY);
    assertThat(concurrent.isDone()).isFalse();
    assertThat(loader.loads.get()).isEqualTo(2);

    loader.lastFuture().complete("v2");
    assertThat(waiting.join()).isEqualTo("v2");
    assertThat(concurrent.join()).isEqualTo("v2");
    assertThat(cache.get(KEY).join()).isEqualTo("v2");
    assertThat(loader.loads.get()).isEqualTo(2);
  }

  @Test
  void refresh_failureWithinStaleWindow_keepsOldValueAndRetries() {
    prime("v1");

    ticker.advance(Duration.ofSeconds(90));
    assertThat(cache.get(KEY).join()).isEqualTo("v1");
    assertThat(loader.loads.get()).isEqualTo(2);

    loader.lastFuture().completeExceptionally(new RuntimeException("boom"));

    // the failed refresh leaves the old value in place; the next read serves it and retries
    var retried = cache.get(KEY);
    assertThat(retried.isDone()).isTrue();
    assertThat(retried.join()).isEqualTo("v1");
    assertThat(loader.loads.get()).isEqualTo(3);

    loader.lastFuture().complete("v2");
    assertThat(cache.get(KEY).join()).isEqualTo("v2");
    assertThat(loader.loads.get()).isEqualTo(3);
  }

  @Test
  void refresh_failureBeyondStalenessCap_propagatesToCaller() {
    prime("v1");

    ticker.advance(Duration.ofSeconds(150));
    var waiting = cache.get(KEY);
    assertThat(waiting.isDone()).isFalse();
    assertThat(loader.loads.get()).isEqualTo(2);

    var failure = new IllegalStateException("boom");
    loader.lastFuture().completeExceptionally(failure);
    var thrown = assertThrows(ExecutionException.class, waiting::get);
    assertThat(thrown).hasCauseThat().isSameInstanceAs(failure);

    // the failure is not cached as a value; the next read starts a new load and waits
    var retry = cache.get(KEY);
    assertThat(retry.isDone()).isFalse();
    assertThat(loader.loads.get()).isEqualTo(3);

    loader.lastFuture().complete("v2");
    assertThat(retry.join()).isEqualTo("v2");
  }

  @Test
  void get_concurrentReads_shareSingleLoad() {
    int threads = 8;
    var result = ConcurrentTestHarness.timeTasks(threads, () -> cache.get(KEY));

    assertThat(loader.loads.get()).isEqualTo(1);
    loader.lastFuture().complete("v1");
    for (var future : result.results()) {
      assertThat(requireNonNull(future).join()).isEqualTo("v1");
    }
    assertThat(loader.loads.get()).isEqualTo(1);
  }

  @Test
  void invalidate_duringRefresh_discardsRefreshResult() {
    prime("v1");

    ticker.advance(Duration.ofSeconds(90));
    assertThat(cache.get(KEY).join()).isEqualTo("v1");
    assertThat(loader.loads.get()).isEqualTo(2);

    cache.invalidate(KEY);
    loader.lastFuture().complete("v2"); // the in-flight refresh completes after invalidation

    assertThat(cache.getIfPresent(KEY)).isNull(); // the discarded refresh is not resurrected
    var reload = cache.get(KEY);
    assertThat(reload.isDone()).isFalse();
    assertThat(loader.loads.get()).isEqualTo(3);

    loader.lastFuture().complete("v3");
    assertThat(reload.join()).isEqualTo("v3");
    assertThat(cache.getIfPresent(KEY)).isEqualTo("v3");
  }

  @Test
  void refresh_oldLoadCompletesLate_doesNotOverwriteNewerValue() {
    prime("v1");

    ticker.advance(Duration.ofSeconds(90));
    assertThat(cache.get(KEY).join()).isEqualTo("v1"); // starts the background refresh
    assertThat(loader.loads.get()).isEqualTo(2);

    cache.invalidate(KEY);
    var reload = cache.get(KEY); // a fresh load starts on a new entry
    assertThat(loader.loads.get()).isEqualTo(3);

    loader.futures.get(2).complete("v2"); // the newer load wins
    assertThat(reload.join()).isEqualTo("v2");

    loader.futures.get(1).complete("stale"); // the superseded load completes late
    assertThat(cache.get(KEY).join()).isEqualTo("v2");
    assertThat(loader.loads.get()).isEqualTo(3);
  }

  /** A loader that records its invocations and returns manually completed futures. */
  private static final class ManualLoader implements AsyncCacheLoader<String, String> {
    final AtomicInteger loads = new AtomicInteger();
    final List<CompletableFuture<String>> futures = new ArrayList<>();
    volatile CompletableFuture<String> last = new CompletableFuture<>();

    @Override
    public CompletableFuture<String> asyncLoad(String key, Executor executor) {
      loads.incrementAndGet();
      var future = new CompletableFuture<String>();
      futures.add(future);
      last = future;
      return future;
    }

    CompletableFuture<String> lastFuture() {
      return last;
    }
  }
}
