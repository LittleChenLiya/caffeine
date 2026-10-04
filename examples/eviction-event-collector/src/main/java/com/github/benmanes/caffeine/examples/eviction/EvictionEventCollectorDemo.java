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
package com.github.benmanes.caffeine.examples.eviction;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Demonstrates composing an {@link EvictionEventCollector} with a Caffeine cache to hand eviction
 * events to a caller in bounded, immutable batches with at-least-once delivery.
 * <p>
 * Run with: {@code ./gradlew run} (or your IDE).
 */
public final class EvictionEventCollectorDemo {

  private EvictionEventCollectorDemo() {
  }

  public static void main(String[] args) {
    // Bounded collection space: at most 2 events held across all unacknowledged batches.
    EvictionEventCollector<Integer, String> collector = EvictionEventCollector.create(2);

    Cache<Integer, String> cache = Caffeine.newBuilder()
        .maximumSize(2)
        .executor(Runnable::run)
        .removalListener(collector)
        .build();

    // Fill the cache past its size; capacity evictions emit SIZE events. With only 2 slots of
    // collection space, the first 2 events are held and the rest are dropped synchronously.
    for (int i = 0; i < 6; i++) {
      cache.put(i, "value-" + i);
    }
    System.out.println("after 6 puts: dropped=" + collector.droppedCount()
        + ", held=" + collector.pendingEventCount());

    // Drain the first batch. A processing failure is simulated: the batch is re-fetched and the
    // re-fetched snapshot is identical. Only after acknowledgment is capacity released.
    EvictionBatch<Integer, String> first = collector.pollBatch();
    System.out.println("dispatched batch " + first.batchNumber() + ": " + first.events());
    EvictionBatch<Integer, String> refetched = collector.pollBatch();
    System.out.println("re-fetched (failure recovery): same snapshot = " + (refetched == first));
    collector.acknowledge(first.batchNumber());
    System.out.println("acknowledged batch " + first.batchNumber()
        + " (held=" + collector.pendingEventCount() + ")");

    // New events after acknowledgment form the next batch.
    for (int i = 6; i < 8; i++) {
      cache.put(i, "value-" + i);
    }
    EvictionBatch<Integer, String> second = collector.pollBatch();
    System.out.println("dispatched batch " + second.batchNumber() + ": " + second.events());
    collector.acknowledge(second.batchNumber());

    System.out.println("total dropped events: " + collector.droppedCount());
    System.out.println("collector: " + collector);
  }
}
