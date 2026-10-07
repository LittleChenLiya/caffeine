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

import static com.github.benmanes.caffeine.cache.Caffeine.requireArgument;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * A {@link LoadingCache} decorator that memoizes selected load failures so that a key whose load
 * recently failed is rejected without invoking the loader again.
 * <p>
 * When a load through {@link #get} fails with an exception accepted by the {@code memorizable}
 * predicate, the exception is recorded in a failure cache that is separate from the delegate's
 * value cache. While that failure is retained, calls for the same key rethrow the original
 * exception instance (so the cause stays visible) instead of loading, and are counted by
 * {@link #failureHits}. Successful values are stored only in the delegate cache; failure entries
 * expire after {@code failureTtl} and are bounded by {@code maximumFailures}.
 * <p>
 * An explicit {@link #invalidate} or {@link #invalidateAll} discards both the memoized failure
 * and the delegate's value. A {@link #put} or a successful {@link #refresh} also supersedes a
 * memoized failure. A failure whose load was already in flight when an invalidation occurred is
 * discarded when the load completes, so it cannot shadow a value loaded after the invalidation.
 * <p>
 * Only failures of the delegate's cache loader are memoized. Exceptions thrown by a
 * {@code mappingFunction} argument or by a bulk load are not recorded, and a failed refresh is
 * logged and swallowed per the {@link LoadingCache} contract. Memoized failures are not values:
 * {@link #getIfPresent} and {@link #getAllPresent} do not observe them, and mutations through
 * {@link #asMap} bypass the failure cache.
 * <p>
 * This decorator is thread-safe and may be shared by multiple threads.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
public final class FailureMemoizingLoadingCache<K, V extends @Nullable Object>
    implements LoadingCache<K, V> {

  /*
   * A failure entry carries the invalidation generation in which its load started. An
   * invalidation bumps the generation before clearing the failure cache, so a failure whose
   * load was in flight during an invalidation is stale: readers reject it on the generation
   * mismatch even if they observe the entry before the invalidation's clear or the recorder's
   * recheck removes it, and a value loaded after the invalidation is never shadowed by an older
   * failure. Dropping a fresh failure due to an unrelated invalidation is safe: the next get
   * simply retries the load. The failure cache performs maintenance on the writing thread so
   * that the capacity bound is enforced promptly.
   */

  private final LoadingCache<K, V> delegate;
  private final Cache<K, MemoizedFailure> failures;
  private final Predicate<? super Throwable> memorizable;
  private final AtomicLong generation;
  private final LongAdder failureHits;

  private FailureMemoizingLoadingCache(LoadingCache<K, V> delegate,
      Predicate<? super Throwable> memorizable, Duration failureTtl, long maximumFailures,
      Ticker ticker) {
    this.delegate = requireNonNull(delegate);
    this.memorizable = requireNonNull(memorizable);
    this.generation = new AtomicLong();
    this.failureHits = new LongAdder();
    this.failures = Caffeine.newBuilder()
        .ticker(requireNonNull(ticker))
        .expireAfterWrite(requireNonNull(failureTtl))
        .maximumSize(maximumFailures)
        .executor(Runnable::run)
        .build();
  }

  /**
   * Returns a loading cache that memoizes the delegate's load failures, using the system ticker
   * for expiration.
   *
   * @param delegate the cache that loads and stores successful values
   * @param memorizable whether a load failure should be memoized
   * @param failureTtl how long a failure is memoized
   * @param maximumFailures the maximum number of memoized failures retained
   * @param <K> the type of keys maintained by the cache
   * @param <V> the type of mapped values
   * @return a loading cache that memoizes the delegate's load failures
   */
  public static <K, V extends @Nullable Object> FailureMemoizingLoadingCache<K, V> memoizeFailures(
      LoadingCache<K, V> delegate, Predicate<? super Throwable> memorizable, Duration failureTtl,
      long maximumFailures) {
    return memoizeFailures(
        delegate, memorizable, failureTtl, maximumFailures, Ticker.systemTicker());
  }

  /**
   * Returns a loading cache that memoizes the delegate's load failures.
   *
   * @param delegate the cache that loads and stores successful values
   * @param memorizable whether a load failure should be memoized
   * @param failureTtl how long a failure is memoized
   * @param maximumFailures the maximum number of memoized failures retained
   * @param ticker the time source used to expire memoized failures
   * @param <K> the type of keys maintained by the cache
   * @param <V> the type of mapped values
   * @return a loading cache that memoizes the delegate's load failures
   */
  public static <K, V extends @Nullable Object> FailureMemoizingLoadingCache<K, V> memoizeFailures(
      LoadingCache<K, V> delegate, Predicate<? super Throwable> memorizable, Duration failureTtl,
      long maximumFailures, Ticker ticker) {
    requireArgument(maximumFailures > 0, "maximumFailures must be positive");
    requireArgument(
        !requireNonNull(failureTtl).isZero() && !failureTtl.isNegative(),
        "failureTtl must be positive");
    return new FailureMemoizingLoadingCache<>(
        delegate, memorizable, failureTtl, maximumFailures, ticker);
  }

  /**
   * Returns the number of calls rejected by a memoized failure without invoking the loader.
   *
   * @return the number of calls served by a memoized failure
   */
  public long failureHits() {
    return failureHits.sum();
  }

  /**
   * Returns the approximate number of memoized failures currently retained.
   *
   * @return the estimated number of memoized failures
   */
  public long memoizedFailureCount() {
    return failures.estimatedSize();
  }

  /**
   * {@inheritDoc}
   * <p>
   * If a memoized failure is present for {@code key}, it is rethrown without invoking the loader.
   */
  @Override public V get(K key) {
    var failure = memoizedFailureIfCurrent(key);
    if (failure != null) {
      failureHits.increment();
      throw propagate(failure);
    }
    long started = generation.get();
    try {
      return delegate.get(key);
    } catch (RuntimeException | Error e) {
      memoizeFailure(key, e, started);
      throw e;
    }
  }

  /** Records the failure if it is memorizable, tagged with the load's generation. */
  private void memoizeFailure(K key, Throwable failure, long started) {
    if (!memorizable.test(failure)) {
      return;
    }
    var entry = new MemoizedFailure(failure, started);
    failures.put(key, entry);
    if (generation.get() != started) {
      failures.asMap().remove(key, entry);
    }
  }

  /** Returns the failure memoized for the key if still current, discarding a stale one. */
  private @Nullable Throwable memoizedFailureIfCurrent(K key) {
    var entry = failures.getIfPresent(requireNonNull(key));
    if (entry == null) {
      return null;
    }
    if (entry.generation == generation.get()) {
      return entry.failure;
    }
    failures.asMap().remove(key, entry);
    return null;
  }

  /**
   * {@inheritDoc}
   * <p>
   * A memoized failure is not a cached value and does not affect this result.
   */
  @Override public @Nullable V getIfPresent(K key) {
    return delegate.getIfPresent(key);
  }

  /**
   * {@inheritDoc}
   * <p>
   * If a memoized failure is present for {@code key}, it is rethrown without invoking
   * {@code mappingFunction}. Failures thrown by {@code mappingFunction} are not memoized.
   */
  @Override public V get(K key, Function<? super K, ? extends V> mappingFunction) {
    var failure = memoizedFailureIfCurrent(key);
    if (failure != null) {
      failureHits.increment();
      throw propagate(failure);
    }
    return delegate.get(key, mappingFunction);
  }

  @Override public Map<K, @NonNull V> getAllPresent(Iterable<? extends K> keys) {
    return delegate.getAllPresent(keys);
  }

  /**
   * {@inheritDoc}
   * <p>
   * If a memoized failure is present for any of the {@code keys}, it is rethrown without
   * invoking the loader. Failures of the bulk load are not memoized because they cannot be
   * attributed to a single key.
   */
  @Override public Map<K, @NonNull V> getAll(Iterable<? extends K> keys) {
    rethrowMemoizedFailure(keys);
    return delegate.getAll(keys);
  }

  /**
   * {@inheritDoc}
   * <p>
   * If a memoized failure is present for any of the {@code keys}, it is rethrown without invoking
   * {@code mappingFunction}. Failures thrown by {@code mappingFunction} are not memoized.
   */
  @Override public Map<K, @NonNull V> getAll(Iterable<? extends K> keys,
      Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends @NonNull V>>
          mappingFunction) {
    rethrowMemoizedFailure(keys);
    return delegate.getAll(keys, mappingFunction);
  }

  /** Rethrows the first memoized failure found for the keys, if any. */
  private void rethrowMemoizedFailure(Iterable<? extends K> keys) {
    for (K key : keys) {
      var failure = memoizedFailureIfCurrent(key);
      if (failure != null) {
        failureHits.increment();
        throw propagate(failure);
      }
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * An explicit put supersedes any memoized failure for {@code key}.
   */
  @Override public void put(K key, @NonNull V value) {
    failures.invalidate(requireNonNull(key));
    delegate.put(key, value);
  }

  /**
   * {@inheritDoc}
   * <p>
   * An explicit put supersedes any memoized failure for the affected keys.
   */
  @Override public void putAll(Map<? extends K, ? extends @NonNull V> map) {
    map.keySet().forEach(failures::invalidate);
    delegate.putAll(map);
  }

  /**
   * {@inheritDoc}
   * <p>
   * The memoized failure for {@code key}, if any, is discarded as well.
   */
  @Override public void invalidate(K key) {
    generation.incrementAndGet();
    failures.invalidate(requireNonNull(key));
    delegate.invalidate(key);
  }

  /**
   * {@inheritDoc}
   * <p>
   * The memoized failures for the {@code keys}, if any, are discarded as well.
   */
  @Override public void invalidateAll(Iterable<? extends K> keys) {
    generation.incrementAndGet();
    keys.forEach(failures::invalidate);
    delegate.invalidateAll(keys);
  }

  /**
   * {@inheritDoc}
   * <p>
   * All memoized failures are discarded as well.
   */
  @Override public void invalidateAll() {
    generation.incrementAndGet();
    failures.invalidateAll();
    delegate.invalidateAll();
  }

  /**
   * {@inheritDoc}
   * <p>
   * A refresh reloads regardless of a memoized failure and, on success, discards it. A failed
   * refresh is not memoized.
   */
  @Override public CompletableFuture<V> refresh(K key) {
    return delegate.refresh(key).whenComplete((value, error) -> {
      if (error == null) {
        failures.invalidate(key);
      }
    });
  }

  /**
   * {@inheritDoc}
   * <p>
   * A refresh reloads regardless of a memoized failure and, on success, discards it. A failed
   * refresh is not memoized.
   */
  @Override public CompletableFuture<Map<K, @NonNull V>> refreshAll(Iterable<? extends K> keys) {
    return delegate.refreshAll(keys).whenComplete((result, error) -> {
      if (result != null) {
        result.keySet().forEach(failures::invalidate);
      }
    });
  }

  @Override public long estimatedSize() {
    return delegate.estimatedSize();
  }

  @Override public CacheStats stats() {
    return delegate.stats();
  }

  @Override public ConcurrentMap<K, @NonNull V> asMap() {
    return delegate.asMap();
  }

  @Override public void cleanUp() {
    failures.cleanUp();
    delegate.cleanUp();
  }

  @Override public Policy<K, @NonNull V> policy() {
    return delegate.policy();
  }

  /** Rethrows the failure without wrapping so that the original cause stays visible. */
  @SuppressWarnings("unchecked")
  private static <T extends Throwable> RuntimeException propagate(Throwable failure) throws T {
    throw (T) failure;
  }

  /** A memoized load failure and the invalidation generation of the load that produced it. */
  private static final class MemoizedFailure {
    final Throwable failure;
    final long generation;

    MemoizedFailure(Throwable failure, long generation) {
      this.failure = failure;
      this.generation = generation;
    }
  }
}
