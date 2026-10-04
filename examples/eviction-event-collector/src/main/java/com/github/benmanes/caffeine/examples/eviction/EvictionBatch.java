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

import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.NullMarked;

/**
 * An immutable snapshot of a batch of eviction events. Each batch carries a monotonically
 * increasing batch number. The event list is unmodifiable and preserves observation order.
 */
@NullMarked
public final class EvictionBatch<K, V> {

  private final long batchNumber;
  private final List<EvictionEvent<K, V>> events;

  public EvictionBatch(long batchNumber, List<EvictionEvent<K, V>> events) {
    if (batchNumber <= 0) {
      throw new IllegalArgumentException("batchNumber must be positive, but was " + batchNumber);
    }
    this.batchNumber = batchNumber;
    this.events = List.copyOf(events);
  }

  public long batchNumber() {
    return batchNumber;
  }

  public List<EvictionEvent<K, V>> events() {
    return events;
  }

  public int size() {
    return events.size();
  }

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
