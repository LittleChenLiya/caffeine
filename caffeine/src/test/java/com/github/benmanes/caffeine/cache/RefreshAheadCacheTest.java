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

import static com.github.benmanes.caffeine.testing.ConcurrentTestHarness.executor;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.testing.Awaits;
import com.google.errorprone.annotations.Var;

/**
 * Acceptance tests for {@link RefreshAheadCache}: a fresh hit, a stale hit that triggers a single
 * background refresh, a read past the stale limit that waits for the shared load, refresh failure,
 * coalesced concurrent reads, invalidate during a refresh, and a late load that must not resurrect
 * an invalidated mapping. A hand-advanced ticker and manually completed futures make every stage
 * deterministic.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class RefreshAheadCacheTest {
  private static final String KEY = "key";
  private static final Duration FRESH = Duration.ofSeconds(10);
  private static final Duration MAX_STALE = Duration.ofSeconds(20);

  private final FakeTicker ticker = new FakeTicker();

  @Test
  void freshHit_returnsValueWithoutLoading() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    assertThat(loader.invocations()).isEqualTo(1);

    ticker.advance(FRESH.toNanos() - 1);
    assertThat(cache.get(KEY)).isEqualTo("v1");
    assertThat(loader.invocations()).isEqualTo(1);
  }

  @Test
  void staleHit_returnsOldValueAndSingleBackgroundRefresh() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");

    // Enter the stale window: old value is served immediately, one background load starts.
    ticker.advance(FRESH.toNanos());
    assertThat(cache.get(KEY)).isEqualTo("v1");
    var refresh = loader.awaitRequest();
    assertThat(loader.invocations()).isEqualTo(2);
    assertThat(refresh.isDone()).isFalse();

    // Up to the last readable nanosecond, reads while the refresh runs return the old value and
    // start no further loads.
    ticker.advance(MAX_STALE.toNanos() - 1);
    assertThat(cache.get(KEY)).isEqualTo("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    assertThat(loader.invocations()).isEqualTo(2);

    // Completing the refresh publishes the new value and starts a new fresh window.
    refresh.complete("v2");
    ticker.advance(1);
    assertThat(cache.get(KEY)).isEqualTo("v2");
    assertThat(loader.invocations()).isEqualTo(2);
  }

  @Test
  void pastStaleLimit_waitsForSharedLoad() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");

    // A stale read at the last readable nanosecond serves the old value and refreshes once.
    ticker.advance(FRESH.toNanos() + MAX_STALE.toNanos() - 1);
    assertThat(cache.get(KEY)).isEqualTo("v1");
    var refresh = loader.awaitRequest();
    refresh.complete("v2");
    ticker.advance(1);

    // The refreshed value later expires: reads must block on a single shared load.
    ticker.advance(FRESH.toNanos() + MAX_STALE.toNanos());
    var blocked = startGet(cache);
    var load = loader.awaitRequest();
    assertThat(loader.invocations()).isEqualTo(3);

    var joined = startGet(cache);
    Awaits.await().untilAsserted(() -> {
      assertThat(loader.invocations()).isEqualTo(3);
      assertThat(blocked.isDone()).isFalse();
      assertThat(joined.isDone()).isFalse();
    });

    load.complete("v3");
    assertThat(getResult(blocked)).isEqualTo("v3");
    assertThat(getResult(joined)).isEqualTo("v3");
    assertThat(cache.get(KEY)).isEqualTo("v3");
    assertThat(loader.invocations()).isEqualTo(3);
  }

  @Test
  void refreshFailure_keepsOldValueThenPropagatesAfterStaleLimit() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);
    var boom = new IllegalStateException("boom");

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");

    // A failed background refresh preserves the old value; the next stale read retries it.
    ticker.advance(FRESH.toNanos());
    assertThat(cache.get(KEY)).isEqualTo("v1");
    var failedRefresh = loader.awaitRequest();
    failedRefresh.completeExceptionally(boom);

    ticker.advance(1);
    assertThat(cache.get(KEY)).isEqualTo("v1");
    var retry = loader.awaitRequest();
    assertThat(loader.invocations()).isEqualTo(3);

    // Once too stale, the failure is handed to waiters instead of the old value.
    ticker.advance(MAX_STALE.toNanos() - 1);
    retry.completeExceptionally(boom);
    var failedGet = startGet(cache);
    var failedLoad = loader.awaitRequest();
    failedLoad.completeExceptionally(boom);
    var thrown = assertThrows(CompletionException.class, () -> getResult(failedGet));
    assertThat(thrown).hasCauseThat().isSameInstanceAs(boom);

    // The old value is not resurrected as fresh; a successful load restores service.
    var recovered = startGet(cache);
    loader.awaitRequest().complete("v2");
    assertThat(getResult(recovered)).isEqualTo("v2");
    assertThat(cache.get(KEY)).isEqualTo("v2");
    assertThat(loader.invocations()).isEqualTo(5);
  }

  @Test
  void concurrentStaleReads_shareOneRefresh() throws Exception {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    ticker.advance(FRESH.toNanos());

    int readers = 8;
    var results = new ArrayList<Future<@Nullable String>>();
    for (int i = 0; i < readers; i++) {
      results.add(executor.submit(() -> cache.get(KEY)));
    }
    Awaits.await().untilAsserted(() -> assertThat(loader.invocations()).isEqualTo(2));
    var shared = loader.awaitRequest();
    assertThat(shared.isDone()).isFalse();

    // Every reader observed the stale value; none waited or started a second load.
    for (var result : results) {
      assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo("v1");
    }
    assertThat(loader.invocations()).isEqualTo(2);

    shared.complete("v2");
    assertThat(cache.get(KEY)).isEqualTo("v2");
  }

  @Test
  void concurrentExpiredReads_shareOneLoad() throws Exception {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    ticker.advance(FRESH.toNanos() + MAX_STALE.toNanos());

    int readers = 8;
    var results = new ArrayList<Future<@Nullable String>>();
    for (int i = 0; i < readers; i++) {
      results.add(executor.submit(() -> cache.get(KEY)));
    }
    Awaits.await().untilAsserted(() -> assertThat(loader.invocations()).isEqualTo(2));
    var shared = loader.awaitRequest();
    for (var result : results) {
      assertThat(result.isDone()).isFalse();
    }

    shared.complete("v2");
    for (var result : results) {
      assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo("v2");
    }
    assertThat(loader.invocations()).isEqualTo(2);
  }

  @Test
  void invalidateDuringLoad_cancelsWaitersAndDiscardsLateLoad() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    ticker.advance(FRESH.toNanos() + MAX_STALE.toNanos());

    // A read beyond the stale limit waits on the load; invalidate cancels it.
    var blocked = startGet(cache);
    var load = loader.awaitRequest();
    cache.invalidate(KEY);
    assertThrows(CancellationException.class, () -> getResult(blocked));

    // The late result of the invalidated load must not repopulate the mapping.
    load.complete("v2");
    loader.prime("v3");
    assertThat(cache.get(KEY)).isEqualTo("v3");
    assertThat(loader.invocations()).isEqualTo(3);
  }

  @Test
  void invalidateWhileStaleRefreshInFlight_discardsLateLoad() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    ticker.advance(FRESH.toNanos());

    assertThat(cache.get(KEY)).isEqualTo("v1");
    var refresh = loader.awaitRequest();
    cache.invalidate(KEY);
    refresh.complete("v2");

    loader.prime("v3");
    assertThat(cache.get(KEY)).isEqualTo("v3");
    assertThat(loader.invocations()).isEqualTo(3);
  }

  @Test
  void invalidatedThenReloaded_oldLoadCompletingLateDoesNotReplaceNewValue() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    ticker.advance(FRESH.toNanos());

    // A stale read starts a background refresh; invalidate ends that entry's load sequence.
    assertThat(cache.get(KEY)).isEqualTo("v1");
    var first = loader.awaitRequest();
    cache.invalidate(KEY);

    // A new mapping is loaded while the old load is still pending.
    loader.prime("v3");
    assertThat(cache.get(KEY)).isEqualTo("v3");
    var second = loader.awaitInvocation();
    assertThat(second).isNotSameInstanceAs(first);
    assertThat(loader.invocations()).isEqualTo(3);

    // Completing the old load cannot overwrite the new value or repopulate a removed mapping.
    first.complete("v2");
    assertThat(cache.get(KEY)).isEqualTo("v3");
    assertThat(loader.invocations()).isEqualTo(3);
  }

  @Test
  void nullLoadResult_isNotCached() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime(null);
    assertThat(cache.get(KEY)).isNull();
    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    assertThat(loader.invocations()).isEqualTo(2);
  }

  @Test
  void nullRefreshResult_removesStaleMapping() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);

    loader.prime("v1");
    assertThat(cache.get(KEY)).isEqualTo("v1");
    ticker.advance(FRESH.toNanos());

    assertThat(cache.get(KEY)).isEqualTo("v1");
    var refresh = loader.awaitRequest();
    loader.complete(refresh, null);

    // The stale value was removed, so the next read loads the mapping from scratch.
    loader.prime("v2");
    assertThat(cache.get(KEY)).isEqualTo("v2");
    assertThat(loader.invocations()).isEqualTo(3);
  }

  @Test
  void loadError_isRethrownDirectly() {
    var loader = new ManualLoader<String, String>();
    var cache = newCache(loader);
    var boom = new StackOverflowError("boom");

    var blocked = startGet(cache);
    var load = loader.awaitRequest();
    load.completeExceptionally(boom);
    var thrown = assertThrows(StackOverflowError.class, () -> getResult(blocked));
    assertThat(thrown).isSameInstanceAs(boom);
  }

  private RefreshAheadCache<String, String> newCache(ManualLoader<String, String> loader) {
    return RefreshAheadCache.<String, String>newBuilder()
        .ticker(ticker)
        .executor(executor)
        .refreshAfterWrite(FRESH)
        .staleAfterRefresh(MAX_STALE)
        .loader(loader)
        .build();
  }

  private static Future<@Nullable String> startGet(RefreshAheadCache<String, String> cache) {
    return executor.submit(() -> cache.get(KEY));
  }

  private static @Nullable String getResult(Future<@Nullable String> future) {
    try {
      return future.get(5, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      var cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new CompletionException(cause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CompletionException(e);
    } catch (TimeoutException e) {
      throw new AssertionError("timed out waiting for get()", e);
    }
  }

  /** A ticker whose time only advances when the test advances it. */
  private static final class FakeTicker implements Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long read() {
      return nanos.get();
    }

    void advance(long deltaNanos) {
      nanos.addAndGet(deltaNanos);
    }
  }

  /**
   * A loader whose invocations hand out futures. A primed invocation returns an already completed
   * future; any other invocation hands a new, incomplete future to the blocking gate, so the test
   * triggers the load first and only then completes the exact future the cache is waiting on.
   */
  private static final class ManualLoader<K, V extends @Nullable Object>
      implements AsyncCacheLoader<K, V> {
    private final BlockingQueue<CompletableFuture<V>> primed = new LinkedBlockingQueue<>();
    private final BlockingQueue<CompletableFuture<V>> requests = new LinkedBlockingQueue<>();
    private final BlockingQueue<CompletableFuture<V>> invoked = new LinkedBlockingQueue<>();
    private final AtomicInteger invocations = new AtomicInteger();

    /** Enqueues a pre-completed future returned by the next invocation. */
    @SuppressWarnings("NullAway") // the loader's value type is nullable
    void prime(@Nullable V value) {
      var future = new CompletableFuture<V>();
      future.complete(value);
      primed.add(future);
    }

    /** Waits for the next unprimed invocation and returns the future it handed out. */
    CompletableFuture<V> awaitRequest() {
      try {
        var request = requests.poll(5, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        return request;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }

    /** Waits for the next invocation of either kind and returns the future it handed out. */
    CompletableFuture<V> awaitInvocation() {
      try {
        var future = invoked.poll(5, TimeUnit.SECONDS);
        assertThat(future).isNotNull();
        return future;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }

    int invocations() {
      return invocations.get();
    }

    /** Completes a loader future with a possibly null value. */
    @SuppressWarnings("NullAway")
    void complete(CompletableFuture<V> future, @Nullable V value) {
      future.complete(value);
    }

    @Override
    public CompletableFuture<V> asyncLoad(K key, Executor executor) {
      invocations.incrementAndGet();
      @Var CompletableFuture<V> future = primed.poll();
      if (future == null) {
        future = new CompletableFuture<V>();
        requests.add(future);
      }
      invoked.add(future);
      return future;
    }
  }
}
