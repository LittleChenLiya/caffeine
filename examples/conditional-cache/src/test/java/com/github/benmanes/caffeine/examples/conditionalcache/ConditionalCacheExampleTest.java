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
package com.github.benmanes.caffeine.examples.conditionalcache;

import static com.google.common.truth.Truth.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.ConditionalCache;
import com.github.benmanes.caffeine.cache.ConditionalCache.Snapshot;

/**
 * Tests for {@link ConditionalCacheExample}.
 */
class ConditionalCacheExampleTest {

  @Test
  void readReturnsValueAndToken() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");

    Snapshot<String> snapshot = cache.getIfPresent(1);
    assertThat(snapshot.value()).isEqualTo("v1");
    assertThat(snapshot.generation()).isGreaterThan(0L);
  }

  @Test
  void replaceWithMatchingTokenCommits() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();

    assertThat(cache.replace(1, token, "v2")).isTrue();
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v2");
  }

  @Test
  void replaceWithStaleTokenIsRejectedAndRetainsValue() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();
    cache.put(1, "v2");

    assertThat(cache.replace(1, token, "v3")).isFalse();
    assertThat(cache.getIfPresent(1).value()).isEqualTo("v2");
  }

  @Test
  void rejectedCandidateIsCleanedUpOnce() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();
    cache.put(1, "v2");

    AtomicInteger cleanups = new AtomicInteger();
    cache.replace(1, token, "v3", candidate -> cleanups.incrementAndGet());
    assertThat(cleanups.get()).isEqualTo(1);
  }

  @Test
  void successfulCommitDoesNotCleanUp() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "v1");
    long token = cache.getIfPresent(1).generation();

    AtomicInteger cleanups = new AtomicInteger();
    cache.replace(1, token, "v2", candidate -> cleanups.incrementAndGet());
    assertThat(cleanups.get()).isEqualTo(0);
  }

  @Test
  void computeTransformsAndCommitsWhenGenerationMatches() {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();
    cache.put(1, "hello");

    cache.compute(1, (key, current) -> current + " world", candidate -> {
    });
    assertThat(cache.getIfPresent(1).value()).isEqualTo("hello world");
  }
}
