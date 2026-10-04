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

import java.util.Objects;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.errorprone.annotations.CheckReturnValue;

/**
 * An immutable record of a single cache eviction. This is the value handed to callers by an
 * {@link EvictionBatch}; it preserves the original {@link RemovalCause} (for example
 * {@link RemovalCause#SIZE} for capacity eviction or {@link RemovalCause#EXPLICIT} for manual
 * removal) rather than collapsing events to a generic notification.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the key type
 * @param <V> the value type
 */
@NullMarked
@CheckReturnValue
public final class EvictionEvent<K, V> {

  private final @Nullable K key;
  private final @Nullable V value;
  private final RemovalCause cause;

  /**
   * Creates an eviction event.
   *
   * @param key the key represented by this entry, or {@code null} if collected
   * @param value the value represented by this entry, or {@code null} if collected
   * @param cause the reason for which the entry was removed
   */
  public EvictionEvent(@Nullable K key, @Nullable V value, RemovalCause cause) {
    this.key = key;
    this.value = value;
    this.cause = requireNonNull(cause);
  }

  /**
   * Returns the key represented by this entry, or {@code null} if collected.
   *
   * @return the key
   */
  public @Nullable K key() {
    return key;
  }

  /**
   * Returns the value represented by this entry, or {@code null} if collected.
   *
   * @return the value
   */
  public @Nullable V value() {
    return value;
  }

  /**
   * Returns the reason for which the entry was removed.
   *
   * @return the removal cause
   */
  public RemovalCause cause() {
    return cause;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof EvictionEvent<?, ?>)) {
      return false;
    }
    EvictionEvent<?, ?> other = (EvictionEvent<?, ?>) o;
    return Objects.equals(key, other.key)
        && Objects.equals(value, other.value)
        && (cause == other.cause);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, cause);
  }

  @Override
  public String toString() {
    return "EvictionEvent[key=" + key + ", value=" + value + ", cause=" + cause + "]";
  }
}
