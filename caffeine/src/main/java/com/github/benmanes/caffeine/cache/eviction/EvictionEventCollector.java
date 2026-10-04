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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.errorprone.annotations.CheckReturnValue;

/**
 * A composable {@link RemovalListener} that collects eviction events and hands them to a caller in
 * bounded, immutable {@linkplain EvictionBatch batches}. The collector is installed on a cache via
 * {@link com.github.benmanes.caffeine.cache.Caffeine#removalListener(RemovalListener)} and never
 * modifies the cache's eviction algorithm; it only observes the events the cache already emits.
 * <p>
 * <b>Batching and acknowledgment.</b> Events accumulate into the current batch. The caller
 * {@linkplain #pollBatch() polls} a batch to obtain an immutable snapshot carrying a monotonically
 * increasing {@linkplain EvictionBatch#batchNumber() batch number}. A polled batch is
 * <em>outstanding</em>: its events remain held and its capacity is reserved until the caller
 * {@linkplain #acknowledge(long) acknowledges} it. If processing fails, the caller re-fetches the
 * same batch (by polling again, or by {@linkplain #fetchBatch(long) fetching} its number) and
 * receives the same snapshot with the same events in the same order. Only after acknowledgment is
 * the batch's capacity released, and only then does the next batch become available.
 * <p>
 * <b>Bounded space and drop semantics.</b> The collector holds at most {@linkplain #maxCapacity()
 * maxCapacity} events across the current batch and all outstanding batches. When an event arrives
 * and the collector is full, the event is dropped: the {@linkplain #droppedCount() drop count} is
 * incremented synchronously on the calling cache thread, without blocking. Explicit removal and
 * capacity eviction events are counted and dropped alike; the original
 * {@linkplain EvictionEvent#cause() cause} is preserved for every event that is delivered.
 * <p>
 * <b>Concurrency.</b> The collector is safe for concurrent use by multiple cache threads and
 * caller threads. Acknowledgment is idempotent: acknowledging the same batch more than once has no
 * additional effect and never releases capacity twice.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the key type
 * @param <V> the value type
 */
@NullMarked
@CheckReturnValue
public final class EvictionEventCollector<K, V> implements RemovalListener<K, V> {

  /** The batch number assigned to the first dispatched batch. */
  public static final long FIRST_BATCH_NUMBER = 1;

  private final AtomicReference<State<K, V>> state;
  private final int maxCapacity;
  private final @Nullable RemovalListener<K, V> delegate;

  EvictionEventCollector(Builder<K, V> builder) {
    this.maxCapacity = builder.maxCapacity;
    this.delegate = builder.delegate;
    this.state = new AtomicReference<>(State.initial());
  }

  /**
   * Returns a builder for an eviction event collector.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new builder
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  /**
   * Returns a new collector with the given maximum number of held events.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @param maxCapacity the maximum number of events held (current and outstanding batches)
   * @return a new collector
   * @throws IllegalArgumentException if {@code maxCapacity} is not positive
   */
  public static <K, V> EvictionEventCollector<K, V> create(int maxCapacity) {
    return EvictionEventCollector.<K, V>builder().maxCapacity(maxCapacity).build();
  }

  @Override
  public void onRemoval(@Nullable K key, @Nullable V value, RemovalCause cause) {
    EvictionEvent<K, V> event = new EvictionEvent<>(key, value, cause);
    State<K, V> currentState;
    State<K, V> nextState;
    do {
      currentState = state.get();
      if (currentState.held >= maxCapacity) {
        // Full: record the drop synchronously instead of blocking the cache thread.
        nextState = currentState.withDropped();
      } else {
        nextState = currentState.withAppended(event);
      }
    } while (!state.compareAndSet(currentState, nextState));

    RemovalListener<K, V> listener = delegate;
    if (listener != null) {
      listener.onRemoval(key, value, cause);
    }
  }

  /**
   * Dispatches the next batch. If a batch is already outstanding, this re-fetches that same batch
   * (the next batch only becomes available after the outstanding one is acknowledged). Otherwise
   * the current batch is sealed and returned as an immutable snapshot.
   *
   * @return the outstanding or newly sealed batch, or {@code null} if there is no batch to dispatch
   */
  public @Nullable EvictionBatch<K, V> pollBatch() {
    State<K, V> currentState;
    State<K, V> nextState;
    EvictionBatch<K, V> result;
    do {
      currentState = state.get();
      if (!currentState.pending.isEmpty()) {
        // Re-fetch: the oldest dispatched batch is still outstanding.
        return currentState.pending.entrySet().iterator().next().getValue();
      }
      if (currentState.current.isEmpty()) {
        return null;
      }
      long batchNumber = currentState.nextBatchNumber;
      result = new EvictionBatch<>(batchNumber, List.copyOf(currentState.current));
      nextState = currentState.withSealed(result);
    } while (!state.compareAndSet(currentState, nextState));
    return result;
  }

  /**
   * Re-fetches an outstanding batch by its batch number without modifying state.
   *
   * @param batchNumber the batch number
   * @return the outstanding batch, or {@code null} if it is unknown or already acknowledged
   */
  public @Nullable EvictionBatch<K, V> fetchBatch(long batchNumber) {
    return state.get().pending.get(batchNumber);
  }

  /**
   * Acknowledges a batch, releasing its capacity. Acknowledgment is idempotent: acknowledging an
   * unknown or already-acknowledged batch has no effect.
   *
   * @param batchNumber the batch number
   * @return {@code true} if this call released the batch; {@code false} if it was already
   *         acknowledged or is unknown
   */
  public boolean acknowledge(long batchNumber) {
    State<K, V> currentState;
    State<K, V> nextState;
    do {
      currentState = state.get();
      EvictionBatch<K, V> batch = currentState.pending.get(batchNumber);
      if (batch == null) {
        return false;
      }
      nextState = currentState.withAcknowledged(batchNumber, batch.size());
    } while (!state.compareAndSet(currentState, nextState));
    return true;
  }

  /**
   * Returns the total number of events dropped because the collector was full.
   *
   * @return the drop count
   */
  public long droppedCount() {
    return state.get().dropped;
  }

  /**
   * Returns the total number of batches acknowledged so far.
   *
   * @return the acknowledged batch count
   */
  public long acknowledgedCount() {
    return state.get().acknowledged;
  }

  /**
   * Returns the number of events currently held across the current batch and all outstanding
   * batches.
   *
   * @return the held event count
   */
  public int pendingEventCount() {
    return state.get().held;
  }

  /**
   * Returns the number of outstanding (dispatched but not yet acknowledged) batches.
   *
   * @return the outstanding batch count
   */
  public int pendingBatchCount() {
    return state.get().pending.size();
  }

  /**
   * Returns the number of events in the current, not-yet-dispatched batch.
   *
   * @return the current batch size
   */
  public int currentBatchSize() {
    return state.get().current.size();
  }

  /**
   * Returns the maximum number of events that can be held.
   *
   * @return the maximum capacity
   */
  public int maxCapacity() {
    return maxCapacity;
  }

  @Override
  public String toString() {
    State<K, V> s = state.get();
    return "EvictionEventCollector[maxCapacity=" + maxCapacity + ", held=" + s.held
        + ", outstandingBatches=" + s.pending.size() + ", dropped=" + s.dropped
        + ", acknowledged=" + s.acknowledged + "]";
  }

  /**
   * A builder for {@link EvictionEventCollector}.
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  @NullMarked
  public static final class Builder<K, V> {
    private int maxCapacity = 16;
    private @Nullable RemovalListener<K, V> delegate;

    Builder() {
    }

    /**
     * Sets the maximum number of events held across the current batch and all outstanding
     * batches. When an event arrives and the collector is full, it is dropped and the
     * {@linkplain EvictionEventCollector#droppedCount() drop count} is incremented.
     *
     * @param maxCapacity the maximum number of held events
     * @return this builder
     * @throws IllegalArgumentException if {@code maxCapacity} is not positive
     */
    public Builder<K, V> maxCapacity(int maxCapacity) {
      if (maxCapacity <= 0) {
        throw new IllegalArgumentException("maxCapacity must be positive, but was " + maxCapacity);
      }
      this.maxCapacity = maxCapacity;
      return this;
    }

    /**
     * Sets an optional delegate listener that receives every event after it has been recorded,
     * allowing the collector to compose with an existing listener.
     *
     * @param delegate the delegate listener
     * @return this builder
     */
    public Builder<K, V> delegate(RemovalListener<K, V> delegate) {
      this.delegate = requireNonNull(delegate);
      return this;
    }

    /**
     * Returns a new {@link EvictionEventCollector}.
     *
     * @return a new collector
     */
    public EvictionEventCollector<K, V> build() {
      return new EvictionEventCollector<>(this);
    }
  }

  /**
   * An immutable snapshot of the collector's logical state. All transitions are performed by
   * compare-and-set on a single reference, so cache threads never block and the capacity invariant
   * (held <= maxCapacity) is preserved under concurrency.
   */
  private static final class State<K, V> {
    final long nextBatchNumber;
    final List<EvictionEvent<K, V>> current;
    final Map<Long, EvictionBatch<K, V>> pending;
    final int held;
    final long dropped;
    final long acknowledged;

    State(long nextBatchNumber, List<EvictionEvent<K, V>> current,
        Map<Long, EvictionBatch<K, V>> pending, int held, long dropped, long acknowledged) {
      this.nextBatchNumber = nextBatchNumber;
      this.current = current;
      this.pending = pending;
      this.held = held;
      this.dropped = dropped;
      this.acknowledged = acknowledged;
    }

    static <K, V> State<K, V> initial() {
      return new State<>(FIRST_BATCH_NUMBER, List.of(), Map.of(), 0, 0, 0);
    }

    State<K, V> withAppended(EvictionEvent<K, V> event) {
      List<EvictionEvent<K, V>> updated = new ArrayList<>(current.size() + 1);
      updated.addAll(current);
      updated.add(event);
      return new State<>(nextBatchNumber, List.copyOf(updated), pending,
          held + 1, dropped, acknowledged);
    }

    State<K, V> withDropped() {
      return new State<>(nextBatchNumber, current, pending, held, dropped + 1, acknowledged);
    }

    State<K, V> withSealed(EvictionBatch<K, V> batch) {
      Map<Long, EvictionBatch<K, V>> updated = new LinkedHashMap<>(pending);
      updated.put(batch.batchNumber(), batch);
      return new State<>(nextBatchNumber + 1, List.of(), updated, held, dropped, acknowledged);
    }

    State<K, V> withAcknowledged(long batchNumber, int batchSize) {
      Map<Long, EvictionBatch<K, V>> updated = new LinkedHashMap<>(pending);
      updated.remove(batchNumber);
      return new State<>(nextBatchNumber, current, updated,
          held - batchSize, dropped, acknowledged + 1);
    }
  }
}
