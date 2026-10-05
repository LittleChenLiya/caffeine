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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ConditionalCache}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
class ConditionalCacheTest {

  /* ************************ Read + conditional replace ************************ */

  @Test
  void getIfPresent_returnsValueAndToken() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");

    ConditionalCache.Snapshot<String> snapshot = cache.getIfPresent(1);
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.value()).isEqualTo("v1");
    assertThat(snapshot.generation()).isGreaterThan(0L);
  }

  @Test
  void getIfPresent_absent_returnsNull() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    assertThat(cache.getIfPresent(1)).isNull();
  }

  @Test
  void replace_matchingGeneration_commits() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();

    boolean committed = cache.replace(1, token, "v2");
    assertThat(committed).isTrue();
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v2");
  }

  @Test
  void replace_staleGeneration_rejectedAndValueRetained() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();

    cache.put(1, "v2"); // overwrite invalidates the old token

    boolean committed = cache.replace(1, token, "v3");
    assertThat(committed).isFalse();
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v2");
  }

  @Test
  void replace_absentKey_rejected() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    boolean committed = cache.replace(1, 1L, "v1");
    assertThat(committed).isFalse();
    assertThat(cache.getIfPresent(1)).isNull();
  }

  @Test
  void replace_eachCommitBumpsGeneration() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token1 = cache.getIfPresent(1).generation();

    cache.replace(1, token1, "v2");
    long token2 = cache.getIfPresent(1).generation();
    assertThat(token2).isNotEqualTo(token1);

    cache.replace(1, token2, "v3");
    long token3 = cache.getIfPresent(1).generation();
    assertThat(token3).isNotEqualTo(token2);
    assertThat(token3).isGreaterThan(token2);
  }

  /* ************************ Failure cleanup callback ************************ */

  @Test
  void replace_rejectedCandidate_cleanupCalledOnce() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();
    cache.put(1, "v2");

    AtomicInteger cleanups = new AtomicInteger();
    cache.replace(1, token, "v3", v -> cleanups.incrementAndGet());
    assertThat(cleanups.get()).isEqualTo(1);
  }

  @Test
  void replace_successfulCommit_cleanupNotCalled() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();

    AtomicInteger cleanups = new AtomicInteger();
    cache.replace(1, token, "v2", v -> cleanups.incrementAndGet());
    assertThat(cleanups.get()).isEqualTo(0);
  }

  @Test
  void replace_cleanupCalledExactlyOnce_whenRepeatedlyRejected() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();
    cache.put(1, "v2");

    AtomicInteger cleanups = new AtomicInteger();
    for (int i = 0; i < 5; i++) {
      cache.replace(1, token, "candidate-" + i, v -> cleanups.incrementAndGet());
    }
    assertThat(cleanups.get()).isEqualTo(5);
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v2");
  }

  /* ************************ Concurrency ************************ */

  @Test
  void replace_concurrentCandidates_onlyOneSucceeds() throws InterruptedException {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();

    AtomicInteger successes = new AtomicInteger();
    AtomicInteger cleanups = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);

    Runnable candidate = () -> {
      try {
        start.await();
        boolean ok = cache.replace(1, token, "candidate", v -> cleanups.incrementAndGet());
        if (ok) {
          successes.incrementAndGet();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        done.countDown();
      }
    };

    Thread t1 = new Thread(candidate);
    Thread t2 = new Thread(candidate);
    t1.start();
    t2.start();
    start.countDown();
    done.await();

    assertThat(successes.get()).isEqualTo(1);
    assertThat(cleanups.get()).isEqualTo(1);
    assertThat(cache.getIfPresent(1).value()).isEqualTo("candidate");
  }

  /* ************************ Expired generation ************************ */

  @Test
  void replace_afterExpirationAndReload_oldTokenRejected() {
    FakeTicker ticker = new FakeTicker();
    ConditionalCache<Integer, String> cache = ConditionalCache.create(
        Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(1)).ticker(ticker::read));
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();

    ticker.advance(Duration.ofMinutes(2)); // expire the entry

    String loaded = cache.get(1, k -> "v2"); // reload with a fresh generation
    assertThat(loaded).isEqualTo("v2");

    AtomicInteger cleanups = new AtomicInteger();
    boolean committed = cache.replace(1, token, "v3", v -> cleanups.incrementAndGet());
    assertThat(committed).isFalse();
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v2");
    assertThat(cleanups.get()).isEqualTo(1);
  }

  @Test
  void replace_afterExpirationWithoutReload_oldTokenRejected() {
    FakeTicker ticker = new FakeTicker();
    ConditionalCache<Integer, String> cache = ConditionalCache.create(
        Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(1)).ticker(ticker::read));
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();

    ticker.advance(Duration.ofMinutes(2)); // expire the entry

    AtomicInteger cleanups = new AtomicInteger();
    boolean committed = cache.replace(1, token, "v2", v -> cleanups.incrementAndGet());
    assertThat(committed).isFalse();
    assertThat(cache.getIfPresent(1)).isNull();
    assertThat(cleanups.get()).isEqualTo(1);
  }

  /* ************************ Compute + reentrancy ************************ */

  @Test
  void compute_commitsWhenGenerationMatches() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");

    cache.compute(1, (key, current) -> current + "-computed", v -> {
    });
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v1-computed");
  }

  @Test
  void compute_absentKey_createsValue() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    AtomicInteger cleanups = new AtomicInteger();
    cache.compute(1, (key, current) -> {
      assertThat(current).isNull();
      return "created";
    }, v -> cleanups.incrementAndGet());
    assertThat(cache.getIfPresent(1).value()).isEqualTo("created");
    assertThat(cleanups.get()).isEqualTo(0);
  }

  @Test
  void compute_transformReentersCache_noDeadlock() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");

    AtomicInteger reentries = new AtomicInteger();
    cache.compute(1, (key, current) -> {
      // The transform runs outside the cache's internal lock, so reentering is safe.
      ConditionalCache.Snapshot<String> reRead = cache.getIfPresent(1);
      reentries.incrementAndGet();
      assertThat(reRead.value()).isEqualTo("v1");
      return current + "-computed";
    }, v -> {
    });

    assertThat(reentries.get()).isEqualTo(1);
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v1-computed");
  }

  @Test
  void compute_transformModifiesCache_rejectedAndCleanedUp() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");

    AtomicInteger cleanups = new AtomicInteger();
    cache.compute(1, (key, current) -> {
      // A reentrant modification bumps the generation; the later commit must be rejected.
      cache.put(1, "v2");
      return current + "-candidate";
    }, v -> cleanups.incrementAndGet());

    assertThat(cache.getIfPresent(1).value()).isEqualTo("v2");
    assertThat(cleanups.get()).isEqualTo(1);
  }

  @Test
  void replace_cleanupCallbackReenters_noDeadlock() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();
    cache.put(1, "v2");

    AtomicInteger reentries = new AtomicInteger();
    cache.replace(1, token, "v3", v -> {
      // The callback runs after the commit lock is released, so reentering is safe.
      reentries.incrementAndGet();
      cache.getIfPresent(1);
    });
    assertThat(reentries.get()).isEqualTo(1);
  }

  /* ************************ Ordinary get / put ************************ */

  @Test
  void getAndPut_behaveLikeNormalCache() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    assertThat(cache.getIfPresent(1)).isNull();

    cache.put(1, "v1");
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v1");

    assertThat(cache.get(1, k -> "v2")).isEqualTo("v1"); // existing, no load
    assertThat(cache.get(2, k -> "v2")).isEqualTo("v2"); // absent, load
    assertThat(cache.estimatedSize()).isEqualTo(2);

    cache.invalidate(1);
    assertThat(cache.getIfPresent(1)).isNull();
  }

  @Test
  void get_loadedValueBumpsGeneration() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.get(1, k -> "v1");
    long token = cache.getIfPresent(1).generation();
    assertThat(token).isGreaterThan(0L);

    // A conditional replace with the loaded token commits.
    boolean committed = cache.replace(1, token, "v2");
    assertThat(committed).isTrue();
  }

  /** A manually advanceable ticker for deterministic expiration tests. */
  static final class FakeTicker implements Ticker {
    private final AtomicLong nanos = new AtomicLong();

    void advance(Duration duration) {
      nanos.addAndGet(duration.toNanos());
    }

    @Override public long read() {
      return nanos.get();
    }
  }
}
