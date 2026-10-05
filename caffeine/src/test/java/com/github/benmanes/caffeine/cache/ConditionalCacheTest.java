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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.ConditionalCache.GenerationToken;
import com.github.benmanes.caffeine.cache.ConditionalCache.Stamped;
import com.google.common.testing.FakeTicker;

/**
 * Tests for the explicit conditional-replacement layer, {@link ConditionalCache}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class ConditionalCacheTest {

  private static ConditionalCache<String, String> over(Cache<String, String> cache,
      ConcurrentLinkedQueue<String> discarded) {
    return ConditionalCache.over(cache, discarded::add);
  }

  @Test
  void getIfPresent_absent_returnsNull() {
    var layer = over(Caffeine.newBuilder().build(), new ConcurrentLinkedQueue<>());
    assertThat(layer.getIfPresent("missing")).isNull();
  }

  @Test
  void getIfPresent_present_returnsValueAndToken() {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    cache.put("key", "v1");
    var layer = over(cache, new ConcurrentLinkedQueue<>());

    Stamped<String> snapshot = layer.getIfPresent("key");
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.value()).isEqualTo("v1");
    assertThat(snapshot.token()).isNotNull();
  }

  @Test
  void replace_currentToken_commits() {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    cache.put("key", "v1");
    var discarded = new ConcurrentLinkedQueue<String>();
    var layer = over(cache, discarded);

    Stamped<String> snapshot = requireNonNull(layer.getIfPresent("key"));
    assertThat(layer.replace("key", snapshot.token(), "v2")).isTrue();
    assertThat(cache.getIfPresent("key")).isEqualTo("v2");
    assertThat(discarded).isEmpty();
  }

  @Test
  void transform_currentToken_commitsAndReturnsCandidate() {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    cache.put("key", "v1");
    var discarded = new ConcurrentLinkedQueue<String>();
    var layer = over(cache, discarded);

    Stamped<String> snapshot = requireNonNull(layer.getIfPresent("key"));
    String committed = layer.transform("key", snapshot.token(), old -> old + "+");
    assertThat(committed).isEqualTo("v1+");
    assertThat(cache.getIfPresent("key")).isEqualTo("v1+");
    assertThat(discarded).isEmpty();
  }

  @Test
  void staleToken_afterPut_rejectedAndCurrentValuePreserved() {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    cache.put("key", "v1");
    var discarded = new ConcurrentLinkedQueue<String>();
    var layer = over(cache, discarded);

    // Take the token for v1, then a competing write installs v2.
    Stamped<String> snapshot = requireNonNull(layer.getIfPresent("key"));
    cache.put("key", "v2");

    // A candidate carrying the stale token must be rejected and v2 must be preserved.
    assertThat(layer.replace("key", snapshot.token(), "v3")).isFalse();
    assertThat(cache.getIfPresent("key")).isEqualTo("v2");

    assertThat(layer.transform("key", snapshot.token(), old -> old + "+")).isNull();
    assertThat(cache.getIfPresent("key")).isEqualTo("v2");
    // The failed transform candidate is handed to the cleanup callback exactly once.
    assertThat(discarded).containsExactly("v1+");
  }

  @Test
  void staleToken_afterInvalidate_rejected() {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    cache.put("key", "v1");
    var discarded = new ConcurrentLinkedQueue<String>();
    var layer = over(cache, discarded);

    Stamped<String> snapshot = requireNonNull(layer.getIfPresent("key"));
    cache.invalidate("key");

    assertThat(layer.replace("key", snapshot.token(), "v2")).isFalse();
    assertThat(cache.getIfPresent("key")).isNull();
    assertThat(discarded).isEmpty();
  }

  @Test
  void token_boundToKey_rejectedForOtherKey() {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    cache.put("a", "v1");
    cache.put("b", "w1");
    var layer = over(cache, new ConcurrentLinkedQueue<>());

    Stamped<String> snapshot = requireNonNull(layer.getIfPresent("a"));
    assertThat(layer.replace("b", snapshot.token(), "w2")).isFalse();
    assertThat(layer.transform("b", snapshot.token(), old -> old + "+")).isNull();
    assertThat(cache.getIfPresent("b")).isEqualTo("w1");
  }

  @Test
  void expiredGeneration_reloadUnderSameKey_rejectsOldToken() {
    var ticker = new FakeTicker();
    Cache<String, String> cache = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofMinutes(1))
        .ticker(ticker::read)
        .executor(Runnable::run)
        .build();
    var discarded = new ConcurrentLinkedQueue<String>();
    var layer = over(cache, discarded);

    cache.put("key", "v1");
    Stamped<String> expired = requireNonNull(layer.getIfPresent("key"));

    // The entry expires; the old generation is gone.
    ticker.advance(Duration.ofMinutes(2));
    assertThat(layer.getIfPresent("key")).isNull();
    assertThat(layer.replace("key", expired.token(), "v3")).isFalse();
    assertThat(cache.getIfPresent("key")).isNull();

    // The same key is reloaded; tokens from the expired generation must still be rejected.
    cache.put("key", "v2");
    assertThat(layer.replace("key", expired.token(), "v3")).isFalse();
    assertThat(layer.transform("key", expired.token(), old -> old + "+")).isNull();
    assertThat(cache.getIfPresent("key")).isEqualTo("v2");
    assertThat(discarded).containsExactly("v1+");

    // A fresh token for the reloaded generation commits normally.
    Stamped<String> reloaded = requireNonNull(layer.getIfPresent("key"));
    assertThat(layer.replace("key", reloaded.token(), "v4")).isTrue();
    assertThat(cache.getIfPresent("key")).isEqualTo("v4");
  }

  @Test
  void concurrentTransform_exactlyOneCommits_andCleanupCalledOnce() throws Exception {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    cache.put("key", "v1");
    var discarded = new ConcurrentLinkedQueue<String>();
    var layer = over(cache, discarded);

    Stamped<String> snapshot = requireNonNull(layer.getIfPresent("key"));
    GenerationToken token = snapshot.token();

    int contenders = 2;
    var ready = new CountDownLatch(contenders);
    var start = new CountDownLatch(1);
    var results = new CopyOnWriteArrayList<String>();
    var threads = new Thread[contenders];
    for (int i = 0; i < contenders; i++) {
      String candidate = "candidate-" + i;
      threads[i] = new Thread(() -> {
        ready.countDown();
        try {
          start.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        String committed = layer.transform("key", token, old -> candidate);
        if (committed != null) {
          results.add(committed);
        }
      });
      threads[i].start();
    }
    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
    start.countDown();
    for (var thread : threads) {
      thread.join(TimeUnit.SECONDS.toMillis(10));
      assertThat(thread.isAlive()).isFalse();
    }

    // Exactly one candidate commits; the loser is discarded via the cleanup callback once.
    assertThat(results).hasSize(1);
    assertThat(cache.getIfPresent("key")).isEqualTo(results.get(0));
    assertThat(discarded).hasSize(1);
    assertThat(discarded.peek()).isAnyOf("candidate-0", "candidate-1");
    assertThat(discarded.peek()).isNotEqualTo(results.get(0));
  }

  @Test
  void failedCandidate_notReportedAsSuccessfulUpdate() {
    var notifications = new CopyOnWriteArrayList<String>();
    Cache<String, String> cache = Caffeine.newBuilder()
        .executor(Runnable::run)
        .removalListener((key, value, cause) ->
            notifications.add(key + "=" + value + ":" + cause))
        .build();
    var discarded = new ConcurrentLinkedQueue<String>();
    var layer = over(cache, discarded);

    cache.put("key", "v1");
    Stamped<String> snapshot = requireNonNull(layer.getIfPresent("key"));
    cache.put("key", "v2");
    notifications.clear();

    // The losing candidate must not be signaled to listeners as a successful update.
    assertThat(layer.transform("key", snapshot.token(), old -> "loser")).isNull();
    assertThat(notifications).isEmpty();
    assertThat(discarded).containsExactly("loser");

    // A winning candidate is a genuine replacement and is reported normally.
    Stamped<String> current = requireNonNull(layer.getIfPresent("key"));
    assertThat(layer.transform("key", current.token(), old -> "winner")).isEqualTo("winner");
    assertThat(notifications).containsExactly("key=v2:" + RemovalCause.REPLACED);
  }

  @Test
  void cleanupCallback_mayReenterLayerAndCache() {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    cache.put("key", "v1");
    cache.put("other", "o1");
    var reentered = new AtomicInteger();
    var layerRef = new AtomicReference<ConditionalCache<String, String>>();
    var layer = ConditionalCache.<String, String>over(cache, candidate -> {
      // Reentrant use of the layer and the cache from the cleanup callback must not deadlock.
      reentered.incrementAndGet();
      var unusedValue = cache.getIfPresent("key");
      var unusedSnapshot = layerRef.get().getIfPresent("other");
    });
    layerRef.set(layer);

    Stamped<String> snapshot = requireNonNull(layer.getIfPresent("key"));
    cache.put("key", "v2");

    assertTimeoutPreemptively(Duration.ofSeconds(10),
        () -> layer.transform("key", snapshot.token(), old -> "loser"));
    assertThat(reentered.get()).isEqualTo(1);
    assertThat(cache.getIfPresent("key")).isEqualTo("v2");
  }

  @Test
  void plainGetAndPut_behaviorUnchanged() {
    Cache<String, String> cache = Caffeine.newBuilder().build();
    var layer = over(cache, new ConcurrentLinkedQueue<>());

    cache.put("key", "v1");
    assertThat(cache.getIfPresent("key")).isEqualTo("v1");
    assertThat(layer.cache()).isSameInstanceAs(cache);
    assertThat(cache.get("key", k -> "loaded")).isEqualTo("v1");
    cache.put("key", "v2");
    assertThat(cache.getIfPresent("key")).isEqualTo("v2");
    cache.invalidate("key");
    assertThat(cache.getIfPresent("key")).isNull();
  }
}
