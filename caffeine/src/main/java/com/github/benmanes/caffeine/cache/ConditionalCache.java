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
import java.util.function.Consumer;
import java.util.function.Function;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * An explicit conditional-replacement layer over a {@link Cache}. Reads through this layer return
 * the value together with an opaque {@link GenerationToken}, and an update is committed only if
 * the token still belongs to the value currently stored for the key. This provides an
 * optimistic-concurrency (compare-and-set) protocol without holding the cache's internal locks
 * while a new value is being computed.
 * <p>
 * The protocol is:
 * <ol>
 *   <li>{@link #getIfPresent} returns a {@link Stamped} snapshot: the current value and a token
 *       that is bound to that exact value instance for that key.
 *   <li>The caller derives a candidate value, e.g. via {@link #transform}, whose mapping function
 *       executes <em>outside</em> the cache's internal locks.
 *   <li>At commit time the token is re-validated against the current mapping inside a single
 *       atomic map operation. The candidate is installed only if the stored value is still the
 *       identical instance the token was issued for.
 * </ol>
 * A token becomes stale when the mapping changes for any reason: a competing
 * {@code put}/{@code replace}/{@code transform}, an invalidation, or an eviction (including
 * expiration). If an expired entry is reloaded under the same key, the reloaded value is a new
 * generation and tokens issued for the expired value are rejected.
 * <p>
 * A candidate that loses the race is never stored and never reported to the cache's
 * {@code RemovalListener} as a successful update. Instead, a candidate produced by
 * {@link #transform} that fails to commit is handed exactly once to the
 * {@code discardedCandidateHandler} supplied at construction. That callback is invoked outside
 * the cache's internal locks, so it may safely re-enter this layer or the underlying cache.
 * <p>
 * Plain {@link Cache#getIfPresent}, {@link Cache#get}, {@link Cache#put}, and all other
 * operations on the {@linkplain #cache() underlying cache} are unaffected by this layer.
 * <p>
 * Usage example:
 * <pre>{@code
 *   Cache<String, AtomicInteger> cache = Caffeine.newBuilder().build();
 *   ConditionalCache<String, AtomicInteger> conditional =
 *       ConditionalCache.over(cache, discarded -> release(discarded));
 *
 *   ConditionalCache.Stamped<AtomicInteger> snapshot = conditional.getIfPresent("key");
 *   if (snapshot != null) {
 *     // The increment is computed outside the cache's internal locks and committed only
 *     // if no one else changed the mapping since the snapshot was taken.
 *     AtomicInteger updated = conditional.transform("key", snapshot.token(), current -> {
 *       var copy = new AtomicInteger(current.get());
 *       copy.incrementAndGet();
 *       return copy;
 *     });
 *     if (updated == null) {
 *       // The token was stale: the candidate was handed to the cleanup callback instead.
 *     }
 *   }
 * }</pre>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the type of keys maintained by the underlying cache
 * @param <V> the type of mapped values
 */
@NullMarked
public final class ConditionalCache<K, V extends @Nullable Object> {
  private final Cache<K, V> cache;
  private final Consumer<? super V> discardedCandidateHandler;
  private final AtomicLong sequence;

  private ConditionalCache(Cache<K, V> cache, Consumer<? super V> discardedCandidateHandler) {
    this.cache = requireNonNull(cache);
    this.discardedCandidateHandler = requireNonNull(discardedCandidateHandler);
    this.sequence = new AtomicLong();
  }

  /**
   * Returns a conditional-replacement layer over the given cache.
   *
   * @param <K> the type of keys maintained by the cache
   * @param <V> the type of mapped values
   * @param cache the backing cache; its regular operations are unaffected
   * @param discardedCandidateHandler the callback invoked exactly once with each candidate
   *     produced by {@link #transform} that failed to commit; invoked outside the cache's
   *     internal locks and therefore permitted to re-enter the cache
   * @return a conditional-replacement layer over {@code cache}
   * @throws NullPointerException if any argument is null
   */
  public static <K, V extends @Nullable Object> ConditionalCache<K, V> over(
      Cache<K, V> cache, Consumer<? super V> discardedCandidateHandler) {
    return new ConditionalCache<>(cache, discardedCandidateHandler);
  }

  /**
   * Returns the underlying cache. All of its regular operations retain their usual behavior.
   *
   * @return the backing cache
   */
  public Cache<K, V> cache() {
    return cache;
  }

  /**
   * Returns the value associated with the {@code key} together with a generation token, or
   * {@code null} if there is no cached value for the {@code key}. The token is bound to the
   * returned value instance and can be used with {@link #replace} or {@link #transform} to
   * conditionally update the mapping.
   *
   * @param key the key whose associated value is to be returned
   * @return the current value and its generation token, or {@code null} if the cache does not
   *     contain a mapping for the key
   * @throws NullPointerException if the specified key is null
   */
  public @Nullable Stamped<V> getIfPresent(K key) {
    V value = cache.getIfPresent(requireNonNull(key));
    if (value == null) {
      return null;
    }
    return new Stamped<>(value, new GenerationToken(key, value, sequence.incrementAndGet()));
  }

  /**
   * Conditionally replaces the value associated with the {@code key} with {@code newValue},
   * committing only if the {@code token} still belongs to the value currently stored for the key.
   * The token is re-validated inside a single atomic map operation; no cache locks are held by
   * this layer beforehand.
   * <p>
   * If the token is stale, the mapping is left untouched, no listener is notified, and this
   * method returns {@code false}. The supplied {@code newValue} remains owned by the caller and
   * is <em>not</em> passed to the discarded-candidate handler.
   *
   * @param key the key with which the specified value is to be associated
   * @param token a token previously obtained from {@link #getIfPresent} for the same key
   * @param newValue the value to install if the token is still current
   * @return {@code true} if the replacement was committed, or {@code false} if the token was
   *     stale and the mapping was left unchanged
   * @throws NullPointerException if any argument is null
   */
  public boolean replace(K key, GenerationToken token, V newValue) {
    requireNonNull(key);
    requireNonNull(token);
    requireNonNull(newValue);
    if (!token.isBoundTo(key)) {
      return false;
    }
    Object expected = token.expectedValue();
    var committed = new boolean[1];
    cache.asMap().compute(key, (k, current) -> {
      if (current == expected) {
        committed[0] = true;
        return newValue;
      }
      return current;
    });
    return committed[0];
  }

  /**
   * Conditionally replaces the value associated with the {@code key} with the result of applying
   * the {@code transformFunction} to the value the {@code token} was issued for. The
   * {@code transformFunction} is evaluated <em>outside</em> the cache's internal locks; the
   * resulting candidate is committed inside a single atomic map operation that re-validates the
   * token against the value currently stored for the key.
   * <p>
   * If the token is stale at commit time, the mapping is left untouched, no listener is
   * notified, the candidate is passed exactly once to the discarded-candidate handler (invoked
   * outside the cache's internal locks, after the commit attempt has completed), and this method
   * returns {@code null}.
   *
   * @param key the key with which the computed value is to be associated
   * @param token a token previously obtained from {@link #getIfPresent} for the same key
   * @param transformFunction the function to compute a candidate from the token's value; must
   *     not return null
   * @return the committed candidate, or {@code null} if the token was stale and the candidate
   *     was discarded
   * @throws NullPointerException if any argument is null or the transformFunction returns null
   */
  public @Nullable V transform(K key, GenerationToken token,
      Function<? super V, ? extends V> transformFunction) {
    requireNonNull(key);
    requireNonNull(token);
    requireNonNull(transformFunction);
    if (!token.isBoundTo(key)) {
      return null;
    }
    @SuppressWarnings("unchecked")
    var expected = (V) token.expectedValue();
    // The mapping function runs outside the cache's internal locks.
    V candidate = requireNonNull(transformFunction.apply(expected),
        "transformFunction returned null");

    // Commit atomically, re-validating that the token still belongs to the current value.
    var committed = new boolean[1];
    cache.asMap().compute(key, (k, current) -> {
      if (current == expected) {
        committed[0] = true;
        return candidate;
      }
      return current;
    });
    if (committed[0]) {
      return candidate;
    }
    // The candidate lost the race: hand it to the cleanup callback exactly once, outside any
    // cache lock, and never signal it as a successful update.
    discardedCandidateHandler.accept(candidate);
    return null;
  }

  /**
   * A snapshot of a cached value together with the {@link GenerationToken} that identifies its
   * generation.
   *
   * @param <V> the type of the cached value
   */
  public static final class Stamped<V extends @Nullable Object> {
    private final V value;
    private final GenerationToken token;

    Stamped(V value, GenerationToken token) {
      this.value = value;
      this.token = token;
    }

    /**
     * Returns the value that was current when this snapshot was taken.
     *
     * @return the snapshot's value
     */
    public V value() {
      return value;
    }

    /**
     * Returns the token bound to {@link #value()} for conditional updates.
     *
     * @return the generation token for this snapshot
     */
    public GenerationToken token() {
      return token;
    }
  }

  /**
   * An opaque token identifying one generation of a key's mapping. A token is valid only while
   * the value it was issued for remains the value stored for its key; any replacement,
   * invalidation, or eviction (including expiration) of that mapping invalidates the token.
   */
  public static final class GenerationToken {
    private final Object key;
    private final Object expectedValue;
    private final long sequence;

    GenerationToken(Object key, Object expectedValue, long sequence) {
      this.key = key;
      this.expectedValue = expectedValue;
      this.sequence = sequence;
    }

    boolean isBoundTo(Object otherKey) {
      return key.equals(otherKey);
    }

    Object expectedValue() {
      return expectedValue;
    }

    @Override
    public String toString() {
      return "GenerationToken[" + sequence + ']';
    }
  }
}
