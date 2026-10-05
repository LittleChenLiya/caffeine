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

import com.github.benmanes.caffeine.cache.ConditionalCache;
import com.github.benmanes.caffeine.cache.ConditionalCache.Snapshot;

/**
 * Demonstrates the explicit conditional replacement API of {@link ConditionalCache}.
 * <p>
 * A read returns a {@link Snapshot} pairing the value with a generation token. An update commits
 * only when the token still belongs to the current value, providing an optimistic concurrency
 * control (compare-and-swap) discipline. A rejected candidate is handed to an independent cleanup
 * callback; a successful commit never invokes it.
 */
public final class ConditionalCacheExample {

  private ConditionalCacheExample() {
  }

  /**
   * Runs the example.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    ConditionalCache<Integer, String> cache = ConditionalCache.create();

    // 1. Read returns the value and a generation token.
    cache.put(1, "v1");
    Snapshot<String> snapshot = cache.getIfPresent(1);
    long token = snapshot.generation();
    System.out.println("Read: value=" + snapshot.value() + ", token=" + token);

    // 2. A conditional replace commits only when the token still matches.
    boolean committed = cache.replace(1, token, "v2");
    System.out.println("Replace with matching token: committed=" + committed);

    // 3. After an overwrite, the old token is rejected and the value is retained.
    cache.put(1, "v3");
    boolean rejected = cache.replace(1, token, "v4");
    System.out.println("Replace with stale token: committed=" + rejected
        + ", retained value=" + cache.getIfPresent(1).value());

    // 4. A rejected candidate is handed to the cleanup callback exactly once.
    cache.put(2, "a");
    long staleToken = cache.getIfPresent(2).generation();
    cache.put(2, "b");
    int[] cleanups = {0};
    cache.replace(2, staleToken, "c", candidate -> {
      cleanups[0]++;
      System.out.println("Cleaning up rejected candidate: " + candidate);
    });
    System.out.println("Cleanup count: " + cleanups[0]);

    // 5. compute transforms outside the lock and re-checks the generation at commit.
    cache.put(3, "hello");
    cache.compute(3, (key, current) -> current + " world", candidate -> {
      // Only invoked if the candidate is rejected.
    });
    System.out.println("Computed value: " + cache.getIfPresent(3).value());
  }
}
