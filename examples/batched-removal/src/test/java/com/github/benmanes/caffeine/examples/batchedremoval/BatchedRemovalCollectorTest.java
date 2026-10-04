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
package com.github.benmanes.caffeine.examples.batchedremoval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.examples.batchedremoval.BatchedRemovalCollector.Batch;
import com.github.benmanes.caffeine.examples.batchedremoval.BatchedRemovalCollector.RemovalEvent;

/**
 * Acceptance tests for {@link BatchedRemovalCollector}. All caches use a direct executor and a
 * maximum size of 2 so that every removal is published synchronously and deterministically.
 */
final class BatchedRemovalCollectorTest {

  private BatchedRemovalCollector<Integer, Integer> collector;
  private Cache<Integer, Integer> cache;

  /** Creates a cache of maximum size 2 that publishes removals to a collector synchronously. */
  private void newCache(int collectorCapacity) {
    collector = BatchedRemovalCollector.create(collectorCapacity);
    cache = Caffeine.newBuilder()
        .maximumSize(2)
        .executor(Runnable::run)
        .removalListener(collector)
        .build();
  }

  /**
   * Triggers three removals: two capacity evictions and one explicit invalidation. With a
   * direct executor and maximum size 2 this deterministically publishes, in order,
   * {@code SIZE(2)}, {@code EXPLICIT(1)}, {@code SIZE(4)}.
   */
  private void triggerThreeRemovals() {
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3); // evicts key 2 with cause SIZE
    cache.invalidate(1); // explicit removal with cause EXPLICIT
    cache.put(4, 4);
    cache.put(5, 5); // evicts key 4 with cause SIZE
  }

  @Test
  void failedProcessingRefetchesSameBatchAndConfirmReleasesNext() {
    newCache(/* collectorCapacity= */ 8);
    triggerThreeRemovals();
    assertEquals(3, collector.bufferedEvents());
    assertEquals(0, collector.dropped());

    // The first batch carries a monotonic id and preserves the original causes in order.
    Batch<Integer, Integer> first = collector.pollBatch(2).orElseThrow();
    assertEquals(1L, first.id());
    assertEquals(
        List.of(
            new RemovalEvent<>(2, 2, RemovalCause.SIZE),
            new RemovalEvent<>(1, 1, RemovalCause.EXPLICIT)),
        first.events());

    // A processing failure does not confirm: re-fetching yields the same id, same events,
    // same order, and no new batch is offered while this one is outstanding.
    Batch<Integer, Integer> refetched = collector.pendingBatch().orElseThrow();
    assertEquals(first.id(), refetched.id());
    assertEquals(first.events(), refetched.events());
    assertTrue(collector.pollBatch(2).isEmpty());

    // Confirmation releases the batch capacity; confirming again is an idempotent no-op.
    assertTrue(collector.confirm(first.id()));
    assertFalse(collector.confirm(first.id()));
    assertEquals(1, collector.bufferedEvents());

    // Only after confirmation does the next batch appear, with the next monotonic id.
    Batch<Integer, Integer> second = collector.pollBatch(2).orElseThrow();
    assertEquals(2L, second.id());
    assertEquals(List.of(new RemovalEvent<>(4, 4, RemovalCause.SIZE)), second.events());
    assertTrue(collector.confirm(second.id()));
    assertEquals(0, collector.bufferedEvents());
  }

  @Test
  void fullBufferDropsIncomingEventsAndCountsThemSynchronously() {
    newCache(/* collectorCapacity= */ 2);
    triggerThreeRemovals();

    // The buffer holds the two oldest events; the third is dropped and counted immediately.
    assertEquals(2, collector.bufferedEvents());
    assertEquals(1, collector.dropped());

    // The dropped event is gone for good; confirming drains the accepted ones in order.
    Batch<Integer, Integer> batch = collector.pollBatch(2).orElseThrow();
    assertEquals(
        List.of(
            new RemovalEvent<>(2, 2, RemovalCause.SIZE),
            new RemovalEvent<>(1, 1, RemovalCause.EXPLICIT)),
        batch.events());
    assertTrue(collector.confirm(batch.id()));
    assertEquals(0, collector.bufferedEvents());
    assertEquals(1, collector.dropped());
  }

  @Test
  void unconfirmedBatchStillOccupiesCapacity() {
    newCache(/* collectorCapacity= */ 2);
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3); // evicts key 2: first removal, buffered
    cache.invalidate(1); // explicit removal: buffer now full
    assertEquals(2, collector.bufferedEvents());

    // Polling does not release capacity: a further removal is dropped while unconfirmed.
    Batch<Integer, Integer> batch = collector.pollBatch(2).orElseThrow();
    cache.put(4, 4);
    cache.put(5, 5); // evicts key 4, but the buffer is still full
    assertEquals(1, collector.dropped());

    // Confirming releases the capacity, so subsequent removals are buffered again.
    assertTrue(collector.confirm(batch.id()));
    cache.put(6, 6); // evicts one entry, buffered
    assertEquals(1, collector.bufferedEvents());
    assertEquals(1, collector.dropped());
  }

  @Test
  void batchSnapshotIsImmutable() {
    newCache(/* collectorCapacity= */ 4);
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3); // triggers a removal
    Batch<Integer, Integer> batch = collector.pollBatch(1).orElseThrow();

    var event = new RemovalEvent<>(9, 9, RemovalCause.EXPLICIT);
    assertThrows(UnsupportedOperationException.class, () -> batch.events().add(event));
    assertThrows(UnsupportedOperationException.class, () -> batch.events().remove(0));
    assertThrows(UnsupportedOperationException.class, batch.events()::clear);

    // The failed mutation attempts did not corrupt the outstanding batch.
    assertEquals(batch.events(), collector.pendingBatch().orElseThrow().events());
  }

  @Test
  void concurrentConfirmsReleaseCapacityExactlyOnce() throws InterruptedException {
    newCache(/* collectorCapacity= */ 4);
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3); // each put beyond the maximum size evicts one entry
    cache.put(4, 4); // two removals, buffered
    assertEquals(2, collector.bufferedEvents());
    Batch<Integer, Integer> batch = collector.pollBatch(2).orElseThrow();

    int threads = 16;
    var ready = new CountDownLatch(threads);
    var start = new CountDownLatch(1);
    var confirmed = new ConcurrentLinkedQueue<Boolean>();
    var pool = Executors.newFixedThreadPool(threads);
    try {
      for (int i = 0; i < threads; i++) {
        pool.execute(() -> {
          ready.countDown();
          try {
            start.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          confirmed.add(collector.confirm(batch.id()));
        });
      }
      assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }

    // Exactly one confirm won the race and the capacity was released a single time.
    assertEquals(threads, confirmed.size());
    assertEquals(1, confirmed.stream().filter(Boolean::booleanValue).count());
    assertEquals(0, collector.bufferedEvents());

    // The released capacity is exactly the batch size: four more events fit, no drops.
    cache.put(6, 6); // each put beyond the maximum size evicts one entry
    cache.put(7, 7);
    cache.put(8, 8);
    cache.put(9, 9);
    assertEquals(4, collector.bufferedEvents());
    assertEquals(0, collector.dropped());
  }

  @Test
  void batchIdsAreMonotonicAcrossRounds() {
    newCache(/* collectorCapacity= */ 8);
    cache.put(1, 1);
    cache.put(2, 2); // cache now full
    for (long round = 1; round <= 3; round++) {
      int key = 100 + (int) round;
      cache.put(key, key); // each put beyond the maximum size evicts one entry
      Batch<Integer, Integer> batch = collector.pollBatch(1).orElseThrow();
      assertEquals(round, batch.id());
      assertEquals(1, batch.events().size());
      assertEquals(RemovalCause.SIZE, batch.events().get(0).cause());
      assertTrue(collector.confirm(batch.id()));
    }
    assertEquals(0, collector.bufferedEvents());
    assertEquals(0, collector.dropped());
  }

  @Test
  void invalidArgumentsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> BatchedRemovalCollector.create(0));
    newCache(/* collectorCapacity= */ 1);
    assertThrows(IllegalArgumentException.class, () -> collector.pollBatch(0));
    assertTrue(collector.pendingBatch().isEmpty());
    assertFalse(collector.confirm(42L)); // unknown id is a no-op
  }
}
