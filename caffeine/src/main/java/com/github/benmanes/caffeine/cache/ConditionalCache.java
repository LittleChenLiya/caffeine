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
package com.github.benmanes.caffeine.cache;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * A cache layer that supports explicit conditional replacement based on <i>generation tokens</i>.
 * <p>
 * A read returns a {@link Snapshot} pairing the current value with the generation token that the
 * value belongs to. An update commits only when the token still belongs to the current value, i.e.
 * the value has not been replaced since the snapshot was taken. This provides an optimistic
 * concurrency control (compare-and-swap) discipline on top of a regular cache.
 * <p>
 * The {@link #compute(Object, BiFunction, Consumer) compute} operation reads the current value
 * under the cache's internal lock, applies the transformation <i>outside</i> of that lock so that a
 * slow or reentrant transformation does not block the cache, and then re-checks the generation at
 * commit time. If the generation no longer matches, the candidate is rejected and handed to an
 * independent failure callback; a successful commit never invokes the callback.
 * <p>
 * The ordinary {@link #get(Object, Function) get} and {@link #put(Object, Object) put} operations
 * behave as they would on a normal cache, except that every loaded or replacement value is assigned
 * a fresh generation so that tokens issued for an older value can never be accepted.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
@NullMarked
public final class ConditionalCache<K, V> {
  private static final long ABSENT = 0L;

  private final Cache<K, Versioned<V>> cache;
  private final AtomicLong generations = new AtomicLong();

  ConditionalCache(Cache<K, Versioned<V>> cache) {
    this.cache = requireNonNull(cache);
  }

  /**
   * Creates a new conditional cache with default settings.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return a new conditional cache
   */
  public static <K, V> ConditionalCache<K, V> create() {
    return create(Caffeine.newBuilder());
  }

  /**
   * Creates a new conditional cache using the supplied specification.
   *
   * @param spec the specification to configure the backing cache
   * @param <K> the key type
   * @param <V> the value type
   * @return a new conditional cache
   */
  public static <K, V> ConditionalCache<K, V> create(CaffeineSpec spec) {
    return create(Caffeine.from(spec));
  }

  /**
   * Creates a new conditional cache using the supplied builder. The builder may be configured with
   * expiration, a custom ticker, and other settings.
   *
   * @param builder the builder to configure the backing cache
   * @param <K> the key type
   * @param <V> the value type
   * @return a new conditional cache
   */
  @SuppressWarnings("unchecked")
  public static <K, V> ConditionalCache<K, V> create(Caffeine<?, ?> builder) {
    return new ConditionalCache<>((Cache<K, Versioned<V>>) builder.build());
  }

  /**
   * Returns a snapshot of the value associated with the {@code key}, or {@code null} if there is no
   * cached value. The snapshot pairs the value with the generation token it belongs to.
   *
   * @param key the key whose associated value is to be returned
   * @return a snapshot of the current value, or {@code null} if absent
   * @throws NullPointerException if the specified key is null
   */
  public @Nullable Snapshot<V> getIfPresent(K key) {
    Versioned<V> versioned = cache.getIfPresent(key);
    return versioned == null ? null
        : new Snapshot<>(versioned.value, versioned.generation);
  }

  /**
   * Returns the value associated with the {@code key}, obtaining that value from the
   * {@code mappingFunction} if necessary. A loaded value is assigned a fresh generation; an
   * existing value is returned unchanged.
   *
   * @param key the key with which the specified value is to be associated
   * @param mappingFunction the function to compute a value
   * @return the current (existing or computed) value associated with the specified key, or null if
   *     the computed value is null
   * @throws NullPointerException if the specified key or mappingFunction is null
   */
  public @Nullable V get(K key, Function<? super K, ? extends V> mappingFunction) {
    requireNonNull(key);
    requireNonNull(mappingFunction);
    Versioned<V> versioned = cache.get(key, k -> {
      V value = mappingFunction.apply(k);
      return value == null ? null : new Versioned<>(value, nextGeneration());
    });
    return versioned == null ? null : versioned.value;
  }

  /**
   * Associates the {@code value} with the {@code key}, replacing any existing value. The new value
   * is assigned a fresh generation, invalidating any token issued for the previous value.
   *
   * @param key the key with which the specified value is to be associated
   * @param value the value to be associated with the specified key
   * @throws NullPointerException if the specified key or value is null
   */
  public void put(K key, V value) {
    requireNonNull(key);
    requireNonNull(value);
    cache.put(key, new Versioned<>(value, nextGeneration()));
  }

  /**
   * Conditionally replaces the value for the {@code key} if the current generation equals
   * {@code expectedGeneration}. This is an atomic compare-and-swap: the generation is re-checked
   * under the cache's internal lock at commit time.
   *
   * @param key the key whose value is to be replaced
   * @param expectedGeneration the generation token the current value must belong to
   * @param newValue the new value to commit
   * @return {@code true} if the replacement was committed; {@code false} if the generation no
   *     longer matched and the candidate was rejected
   * @throws NullPointerException if the specified key or value is null
   */
  public boolean replace(K key, long expectedGeneration, V newValue) {
    return replace(key, expectedGeneration, newValue, ignored -> {});
  }

  /**
   * Conditionally replaces the value for the {@code key} if the current generation equals
   * {@code expectedGeneration}. If the candidate is rejected, it is handed to the
   * {@code failureCleanup} callback exactly once; a successful commit never invokes the callback.
   *
   * @param key the key whose value is to be replaced
   * @param expectedGeneration the generation token the current value must belong to
   * @param newValue the new value to commit
   * @param failureCleanup the callback invoked with the rejected candidate, at most once
   * @return {@code true} if the replacement was committed; {@code false} if the generation no
   *     longer matched and the candidate was rejected
   * @throws NullPointerException if a required argument is null
   */
  public boolean replace(K key, long expectedGeneration, V newValue,
      Consumer<? super V> failureCleanup) {
    requireNonNull(key);
    requireNonNull(newValue);
    requireNonNull(failureCleanup);

    long newGeneration = nextGeneration();
    boolean[] committed = new boolean[1];
    cache.asMap().compute(key, (k, existing) -> {
      if (existing == null || existing.generation != expectedGeneration) {
        return existing;
      }
      committed[0] = true;
      return new Versioned<>(newValue, newGeneration);
    });
    if (!committed[0]) {
      failureCleanup.accept(newValue);
    }
    return committed[0];
  }

  /**
   * Computes a new value for the {@code key} by applying the {@code transform} to the current
   * value, and commits the result only if the generation has not changed since the snapshot was
   * taken. The transform executes outside of the cache's internal lock, so it may safely reenter the
   * cache. If the candidate is rejected, it is handed to the {@code failureCleanup} callback
   * exactly once; a successful commit never invokes the callback.
   * <p>
   * If the key is absent, the transform receives {@code null} and the result is committed only if
   * the key is still absent at commit time.
   *
   * @param key the key whose value is to be computed
   * @param transform the function to compute a candidate value from the current value
   * @param failureCleanup the callback invoked with the rejected candidate, at most once
   * @throws NullPointerException if a required argument is null
   */
  public void compute(K key,
      BiFunction<? super K, ? super @Nullable V, ? extends V> transform,
      Consumer<? super V> failureCleanup) {
    requireNonNull(key);
    requireNonNull(transform);
    requireNonNull(failureCleanup);

    Snapshot<V> current = getIfPresent(key);
    V candidate = transform.apply(key, current == null ? null : current.value);
    if (candidate == null) {
      return;
    }

    long expectedGeneration = current == null ? ABSENT : current.generation;
    long newGeneration = nextGeneration();
    boolean[] committed = new boolean[1];
    cache.asMap().compute(key, (k, existing) -> {
      boolean matches = (existing == null && current == null)
          || (existing != null && current != null && existing.generation == current.generation);
      if (!matches) {
        return existing;
      }
      committed[0] = true;
      return new Versioned<>(candidate, newGeneration);
    });
    if (!committed[0]) {
      failureCleanup.accept(candidate);
    }
  }

  /**
   * Discards any cached value for the {@code key}.
   *
   * @param key the key whose mapping is to be removed from the cache
   * @throws NullPointerException if the specified key is null
   */
  public void invalidate(K key) {
    requireNonNull(key);
    cache.invalidate(key);
  }

  /**
   * Performs any pending maintenance operations needed by the cache.
   */
  public void cleanUp() {
    cache.cleanUp();
  }

  /**
   * Returns the approximate number of entries in this cache.
   *
   * @return the estimated number of mappings
   */
  public long estimatedSize() {
    return cache.estimatedSize();
  }

  private long nextGeneration() {
    return generations.incrementAndGet();
  }

  /**
   * An immutable snapshot of a cached value paired with the generation token it belongs to.
   *
   * @param <V> the value type
   */
  public static final class Snapshot<V> {
    private final V value;
    private final long generation;

    Snapshot(V value, long generation) {
      this.value = requireNonNull(value);
      this.generation = generation;
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public V value() {
      return value;
    }

    /**
     * Returns the generation token the value belongs to.
     *
     * @return the generation token
     */
    public long generation() {
      return generation;
    }

    @Override public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof Snapshot<?> other) {
        return (generation == other.generation) && value.equals(other.value);
      } else {
        return false;
      }
    }

    @Override public int hashCode() {
      return (31 * value.hashCode()) + Long.hashCode(generation);
    }

    @Override public String toString() {
      return "Snapshot{value=" + value + ", generation=" + generation + "}";
    }
  }

  static final class Versioned<V> {
    final V value;
    final long generation;

    Versioned(V value, long generation) {
      this.value = requireNonNull(value);
      this.generation = generation;
    }

    @Override public String toString() {
      return "Versioned{value=" + value + ", generation=" + generation + "}";
    }
  }
}
