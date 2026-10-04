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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

/**
 * A composable {@link RemovalListener} layer that collects removal events from any
 * {@code Caffeine} cache and hands them to the caller in acknowledged batches. The cache
 * eviction algorithm is untouched: this listener only observes the events the cache already
 * publishes, preserving the original {@link RemovalCause} (for example {@code EXPLICIT} for
 * explicit invalidations and {@code SIZE} for capacity evictions).
 *
 * <h2>Batch protocol</h2>
 * <ol>
 *   <li>{@link #pollBatch(int)} forms a batch from the oldest buffered events and returns it
 *       as an immutable snapshot with a monotonically increasing {@linkplain Batch#id() batch
 *       id}. At most one batch is outstanding at a time; while a batch is unconfirmed,
 *       {@code pollBatch} returns {@link Optional#empty()}.</li>
 *   <li>The caller processes the events. If processing fails, {@link #pendingBatch()}
 *       re-fetches the same batch: identical id, identical events, identical order.</li>
 *   <li>{@link #confirm(long)} releases the buffer capacity held by the batch. Confirmation
 *       is idempotent: confirming the same id again (or any stale id) is a no-op, and
 *       concurrent confirms release the capacity exactly once. Only after confirmation can
 *       the next batch be polled.</li>
 * </ol>
 *
 * <h2>Drop semantics</h2>
 * The collection space is bounded by a fixed capacity. Events held by an unconfirmed batch
 * still occupy capacity until confirmed. When the buffer is full, {@link #onRemoval} never
 * blocks the cache thread and never drops a previously accepted event: the incoming event is
 * discarded and the drop is recorded synchronously in the calling thread, observable via
 * {@link #dropped()}. The caller can detect data loss by monitoring that counter and release
 * capacity promptly by confirming batches.
 *
 * @param <K> the type of the key in the cache
 * @param <V> the type of the value in the cache
 */
public final class BatchedRemovalCollector<K, V> implements RemovalListener<K, V> {
  private final ReentrantLock lock = new ReentrantLock();
  private final RemovalEvent<K, V>[] buffer;

  private int head;            // index of the oldest unconsumed event
  private int size;            // unconsumed events, including those held by the pending batch
  private long dropped;        // events discarded because the buffer was full
  private long nextBatchId = 1L;
  private Batch<K, V> pending; // the outstanding unconfirmed batch, or null
  private int pendingSize;     // number of buffer slots held by the pending batch

  private BatchedRemovalCollector(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    @SuppressWarnings("unchecked")
    var events = (RemovalEvent<K, V>[]) new RemovalEvent[capacity];
    buffer = events;
  }

  /** Returns a collector that buffers at most {@code capacity} unconfirmed events. */
  public static <K, V> BatchedRemovalCollector<K, V> create(int capacity) {
    return new BatchedRemovalCollector<>(capacity);
  }

  /**
   * Records a removal event. If the buffer is full the event is discarded and the drop is
   * counted synchronously; this method never blocks the calling (cache) thread.
   */
  @Override
  public void onRemoval(K key, V value, RemovalCause cause) {
    lock.lock();
    try {
      if (size == buffer.length) {
        dropped++;
        return;
      }
      buffer[(head + size) % buffer.length] = new RemovalEvent<>(key, value, cause);
      size++;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Forms a new batch from up to {@code maxEvents} of the oldest buffered events, or returns
   * {@link Optional#empty()} if no events are buffered or a previous batch is still
   * unconfirmed. The returned snapshot is immutable; its capacity is released by
   * {@link #confirm(long)}.
   */
  public Optional<Batch<K, V>> pollBatch(int maxEvents) {
    if (maxEvents <= 0) {
      throw new IllegalArgumentException("maxEvents must be positive");
    }
    lock.lock();
    try {
      if ((pending != null) || (size == 0)) {
        return Optional.empty();
      }
      int count = Math.min(maxEvents, size);
      var events = new ArrayList<RemovalEvent<K, V>>(count);
      for (int i = 0; i < count; i++) {
        events.add(buffer[(head + i) % buffer.length]);
      }
      pending = new Batch<>(nextBatchId++, events);
      pendingSize = count;
      return Optional.of(pending);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the outstanding unconfirmed batch, if any. Re-fetching after a processing failure
   * yields the same batch id and the same events in the same order.
   */
  public Optional<Batch<K, V>> pendingBatch() {
    lock.lock();
    try {
      return Optional.ofNullable(pending);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Releases the buffer capacity held by the batch with the given id and allows the next
   * batch to be polled. Returns {@code true} if this call confirmed the outstanding batch.
   * Confirming an id that is not outstanding, including confirming the same batch twice, is
   * a no-op that returns {@code false}, so concurrent or repeated confirms release the
   * capacity exactly once.
   */
  public boolean confirm(long batchId) {
    lock.lock();
    try {
      if ((pending == null) || (pending.id() != batchId)) {
        return false;
      }
      for (int i = 0; i < pendingSize; i++) {
        buffer[(head + i) % buffer.length] = null;
      }
      head = (head + pendingSize) % buffer.length;
      size -= pendingSize;
      pending = null;
      pendingSize = 0;
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Returns the number of events discarded because the buffer was full. */
  public long dropped() {
    lock.lock();
    try {
      return dropped;
    } finally {
      lock.unlock();
    }
  }

  /** Returns the number of buffered events not yet confirmed, including the pending batch. */
  public int bufferedEvents() {
    lock.lock();
    try {
      return size;
    } finally {
      lock.unlock();
    }
  }

  /** A single removal event, preserving the key, value, and original {@link RemovalCause}. */
  public record RemovalEvent<K, V>(K key, V value, RemovalCause cause) {
    public RemovalEvent {
      requireNonNull(cause, "cause");
    }
  }

  /**
   * An immutable snapshot of buffered events with a monotonically increasing
   * {@linkplain #id() batch id}.
   */
  public record Batch<K, V>(long id, List<RemovalEvent<K, V>> events) {
    public Batch {
      events = List.copyOf(events);
    }
  }
}
