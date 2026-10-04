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
package com.github.benmanes.caffeine.cache.eviction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * Tests for {@link EvictionEventCollector}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class EvictionEventCollectorTest {

  @Test
  void reFetchedBatchHasSameNumberAndContentAndNextAppearsAfterAcknowledgment() {
    EvictionEventCollector<String, String> collector = EvictionEventCollector.create(16);
    Cache<String, String> cache = Caffeine.newBuilder()
        .maximumSize(2)
        .executor(Runnable::run)
        .removalListener(collector)
        .build();

    cache.put("a", "1");
    cache.put("b", "2");
    cache.put("c", "3"); // capacity eviction
    cache.invalidateAll(); // two explicit removals

    EvictionBatch<String, String> first = collector.pollBatch();
    assertEquals(1, first.batchNumber());
    assertEquals(3, first.size());
    // Causes are preserved in observation order: one capacity eviction, two explicit removals.
    assertEquals(List.of(RemovalCause.SIZE, RemovalCause.EXPLICIT, RemovalCause.EXPLICIT),
        first.events().stream().map(EvictionEvent::cause).toList());
    // All three keys are represented, each once.
    assertEquals(3, first.events().stream().map(EvictionEvent::key).distinct().count());

    // Simulated processing failure: re-fetch returns the same batch, same order.
    EvictionBatch<String, String> refetched = collector.pollBatch();
    assertSame(first, refetched);
    assertEquals(1, refetched.batchNumber());
    assertEquals(first.events(), refetched.events());

    // Acknowledgment is idempotent.
    assertTrue(collector.acknowledge(1));
    assertFalse(collector.acknowledge(1));
    assertEquals(0, collector.pendingEventCount());

    // The next batch only appears after acknowledgment.
    cache.put("a", "1");
    cache.put("b", "2");
    cache.put("c", "3"); // capacity eviction
    EvictionBatch<String, String> second = collector.pollBatch();
    assertEquals(2, second.batchNumber());
    assertEquals(1, second.size());
    assertEquals(RemovalCause.SIZE, second.events().get(0).cause());

    // Acknowledge the second batch as well; only then is no batch available.
    assertTrue(collector.acknowledge(2));
    assertNull(collector.pollBatch());
    assertEquals(0, collector.droppedCount());
  }

  @Test
  void dropsWhenFullAndRecordsCountSynchronously() {
    EvictionEventCollector<String, String> collector = EvictionEventCollector.create(2);
    collector.onRemoval("a", "1", RemovalCause.SIZE);
    collector.onRemoval("b", "2", RemovalCause.SIZE);
    collector.onRemoval("c", "3", RemovalCause.SIZE); // dropped

    assertEquals(1, collector.droppedCount());
    assertEquals(2, collector.pendingEventCount());
    assertEquals(2, collector.currentBatchSize());

    EvictionBatch<String, String> first = collector.pollBatch();
    assertEquals(1, first.batchNumber());
    assertEquals(List.of(
        new EvictionEvent<>("a", "1", RemovalCause.SIZE),
        new EvictionEvent<>("b", "2", RemovalCause.SIZE)), first.events());
    assertSame(first, collector.pollBatch()); // still outstanding

    // The outstanding batch still holds capacity, so further events are dropped.
    collector.onRemoval("d", "4", RemovalCause.EXPLICIT);
    assertEquals(2, collector.droppedCount());
    assertEquals(2, collector.pendingEventCount());

    assertTrue(collector.acknowledge(1));
    assertEquals(0, collector.pendingEventCount());

    // Capacity released: new events are accepted and form the next batch.
    collector.onRemoval("d", "4", RemovalCause.EXPLICIT);
    EvictionBatch<String, String> second = collector.pollBatch();
    assertEquals(2, second.batchNumber());
    assertEquals(List.of(new EvictionEvent<>("d", "4", RemovalCause.EXPLICIT)), second.events());
    assertEquals(2, collector.droppedCount());
  }

  @Test
  void snapshotIsImmutable() {
    EvictionEventCollector<String, String> collector = EvictionEventCollector.create(2);
    collector.onRemoval("a", "1", RemovalCause.SIZE);
    EvictionBatch<String, String> batch = collector.pollBatch();

    assertThrows(UnsupportedOperationException.class,
        () -> batch.events().add(new EvictionEvent<>("x", "9", RemovalCause.EXPLICIT)));
    assertThrows(UnsupportedOperationException.class, () -> batch.events().clear());
    assertEquals(1, batch.size());
  }

  @Test
  void fetchBatchReturnsOnlyPendingBatches() {
    EvictionEventCollector<String, String> collector = EvictionEventCollector.create(2);
    collector.onRemoval("a", "1", RemovalCause.SIZE);
    EvictionBatch<String, String> batch = collector.pollBatch();

    assertSame(batch, collector.fetchBatch(1));
    assertNull(collector.fetchBatch(2));
    assertNull(collector.fetchBatch(99));

    assertTrue(collector.acknowledge(1));
    assertNull(collector.fetchBatch(1));
    assertFalse(collector.acknowledge(1));
  }

  @Test
  void concurrentAcknowledgmentReleasesOnlyOnce() throws Exception {
    EvictionEventCollector<String, String> collector = EvictionEventCollector.create(8);
    collector.onRemoval("a", "1", RemovalCause.SIZE);
    collector.onRemoval("b", "2", RemovalCause.SIZE);
    EvictionBatch<String, String> batch = collector.pollBatch();

    int threads = 4;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Boolean>> results = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      results.add(executor.submit(() -> {
        ready.countDown();
        start.await();
        return collector.acknowledge(batch.batchNumber());
      }));
    }
    ready.await(10, TimeUnit.SECONDS);
    start.countDown();

    int released = 0;
    for (Future<Boolean> result : results) {
      if (result.get(10, TimeUnit.SECONDS)) {
        released++;
      }
    }
    assertEquals(1, released);
    assertEquals(0, collector.pendingEventCount());
    assertEquals(1, collector.acknowledgedCount());

    // No double release: the next batch is formed only from newly accepted events.
    collector.onRemoval("c", "3", RemovalCause.EXPLICIT);
    EvictionBatch<String, String> next = collector.pollBatch();
    assertEquals(2, next.batchNumber());
    assertEquals(List.of(new EvictionEvent<>("c", "3", RemovalCause.EXPLICIT)), next.events());

    executor.shutdownNow();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  void delegateReceivesEveryEventAfterRecording() {
    List<EvictionEvent<String, String>> forwarded = new ArrayList<>();
    EvictionEventCollector<String, String> collector = EvictionEventCollector.<String, String>builder()
        .maxCapacity(4)
        .delegate((key, value, cause) -> forwarded.add(new EvictionEvent<>(key, value, cause)))
        .build();

    collector.onRemoval("a", "1", RemovalCause.SIZE);
    collector.onRemoval("b", "2", RemovalCause.EXPLICIT);

    assertEquals(List.of(
        new EvictionEvent<>("a", "1", RemovalCause.SIZE),
        new EvictionEvent<>("b", "2", RemovalCause.EXPLICIT)), forwarded);
  }

  @Test
  void builderRejectsNonPositiveCapacity() {
    assertThrows(IllegalArgumentException.class, () -> EvictionEventCollector.create(0));
    assertThrows(IllegalArgumentException.class, () -> EvictionEventCollector.create(-1));
    assertThrows(IllegalArgumentException.class,
        () -> EvictionEventCollector.<String, String>builder().maxCapacity(0));
  }
}
