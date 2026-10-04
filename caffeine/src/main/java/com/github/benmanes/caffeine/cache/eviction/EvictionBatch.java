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

import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.NullMarked;

import com.google.errorprone.annotations.CheckReturnValue;

/**
 * An immutable snapshot of a batch of eviction events. Each batch carries a monotonically
 * increasing {@linkplain #batchNumber() batch number} that identifies it for re-fetching after a
 * processing failure and for acknowledgment. The event list is unmodifiable and preserves the
 * order in which the events were observed.
 * <p>
 * A batch is <em>outstanding</em> from the moment it is dispatched until it is
 * {@linkplain EvictionEventCollector#acknowledge(long) acknowledged}; its capacity is only
 * released on acknowledgment.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the key type
 * @param <V> the value type
 */
@NullMarked
@CheckReturnValue
public final class EvictionBatch<K, V> {

  private final long batchNumber;
  private final List<EvictionEvent<K, V>> events;

  /**
   * Creates an immutable batch snapshot.
   *
   * @param batchNumber the monotonic batch number
   * @param events the events in this batch
   */
  public EvictionBatch(long batchNumber, List<EvictionEvent<K, V>> events) {
    if (batchNumber <= 0) {
      throw new IllegalArgumentException("batchNumber must be positive, but was " + batchNumber);
    }
    this.batchNumber = batchNumber;
    this.events = List.copyOf(events);
  }

  /**
   * Returns the monotonic batch number.
   *
   * @return the batch number
   */
  public long batchNumber() {
    return batchNumber;
  }

  /**
   * Returns an unmodifiable view of the events in this batch, in observation order.
   *
   * @return the events
   */
  public List<EvictionEvent<K, V>> events() {
    return events;
  }

  /**
   * Returns the number of events in this batch.
   *
   * @return the event count
   */
  public int size() {
    return events.size();
  }

  /**
   * Returns whether this batch contains no events.
   *
   * @return {@code true} if empty
   */
  public boolean isEmpty() {
    return events.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof EvictionBatch<?, ?>)) {
      return false;
    }
    EvictionBatch<?, ?> other = (EvictionBatch<?, ?>) o;
    return (batchNumber == other.batchNumber) && events.equals(other.events);
  }

  @Override
  public int hashCode() {
    return Objects.hash(batchNumber, events);
  }

  @Override
  public String toString() {
    return "EvictionBatch[batchNumber=" + batchNumber + ", events=" + events + "]";
  }
}
