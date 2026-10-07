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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

/**
 * A loading cache adapter that memoizes selected load failures for a short time-to-live, so that
 * a key whose load recently failed is rejected immediately instead of hammering the backing
 * loader.
 * <p>
 * A failure is memoized only when the caller supplied {@linkplain Builder#memoizable classifier}
 * accepts it. While a memoized failure is alive, requests for the same key fail with a
 * {@link MemoizedFailureException} (whose cause is the original error) without invoking the
 * loader. Successful loads are stored in the underlying cache as usual and are unaffected by this
 * mechanism. Failure records are kept in a separate, size-bounded store so that a flood of
 * failing keys cannot exhaust memory; records evicted due to the size bound or expired due to the
 * TTL simply cause the next request to call the loader again.
 * <p>
 * An explicit {@link #invalidate} or {@link #invalidateAll} discards both the cached value and
 * any memoized failure for the affected keys. A failure from a load that was started before an
 * invalidation, but completes afterwards, is discarded and cannot mask a value loaded after the
 * invalidation.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class FailureMemoizingCache<K, V> {
  /** The default capacity bound of the failure store. */
  static final long DEFAULT_MAXIMUM_FAILURES = 1_024;

  /** The cache of successfully loaded values. */
  final AsyncLoadingCache<K, V> cache;
  /** The bounded, short-lived store of memoized failures and invalidation tombstones. */
  final Cache<K, FailureState> failures;
  /** Classifies whether a load failure is eligible for memoization. */
  final Predicate<Throwable> memoizable;
  /** Serializes invalidations with failure recording. */
  final Object invalidationLock = new Object();
  /** Generates tokens identifying the invalidation generation of a key. */
  final AtomicLong tokens = new AtomicLong();
  /** Bumped by {@link #invalidateAll} to discard in-flight failure recordings. */
  final AtomicLong epoch = new AtomicLong();
  /** The number of requests served a memoized failure instead of invoking the loader. */
  final LongAdder failureHitCount = new LongAdder();
  /** The number of load failures that were memoized. */
  final LongAdder memoizedFailureCount = new LongAdder();

  FailureMemoizingCache(Builder<K, V> builder) {
    this.memoizable = builder.memoizable;

    AsyncCacheLoader<? super K, V> delegate = requireNonNull(builder.loader, "loader");
    AsyncCacheLoader<K, V> loader = (key, executor) -> load(delegate, key, executor);
    Caffeine<? super K, ? super V> spec = (builder.successCache == null)
        ? Caffeine.newBuilder().executor(builder.executor)
        : builder.successCache;
    this.cache = spec.buildAsync(loader);
    this.failures = Caffeine.newBuilder()
        .expireAfterWrite(requireNonNull(builder.failureTtl, "failureTtl"))
        .maximumSize(builder.maximumFailures)
        .ticker(builder.ticker)
        .executor(Runnable::run)
        .build();
  }

  /** Returns a future of the value for the key, loading and memoizing any failure if absent. */
  CompletableFuture<V> load(AsyncCacheLoader<? super K, V> delegate, K key, Executor executor) {
    long startEpoch = epoch.get();
    long startToken = currentToken(key);
    CompletableFuture<? extends V> future;
    try {
      future = requireNonNull(delegate.asyncLoad(key, executor));
    } catch (Throwable e) {
      future = CompletableFuture.failedFuture(e);
    }
    @SuppressWarnings("unchecked")
    var result = (CompletableFuture<V>) future.whenComplete((value, error) -> {
      if (error != null) {
        recordFailure(key, unwrap(error), startToken, startEpoch);
      }
    });
    return result;
  }

  /** Records the failure if it is memoizable and the load is still the current generation. */
  void recordFailure(K key, Throwable error, long startToken, long startEpoch) {
    if (!isMemoizable(error)) {
      return;
    }
    synchronized (invalidationLock) {
      if (epoch.get() != startEpoch) {
        return; // invalidateAll() since this load began; the failure is stale
      }
      failures.asMap().compute(key, (k, current) -> {
        if ((current != null) && current.isTombstone() && (current.token != startToken)) {
          return current; // invalidated after this load began; keep the tombstone
        }
        memoizedFailureCount.increment();
        return FailureState.failure(error, startToken);
      });
    }
  }

  /** Returns whether the failure may be memoized, tolerating a misbehaving classifier. */
  boolean isMemoizable(Throwable error) {
    try {
      return memoizable.test(error);
    } catch (RuntimeException | Error e) {
      return false;
    }
  }

  /** Returns the invalidation token currently associated with the key, or 0 if none. */
  long currentToken(K key) {
    FailureState state = failures.getIfPresent(key);
    return (state == null) ? 0L : state.token;
  }

  /**
   * Returns the future value associated with the key. If a memoized failure is alive for the key
   * then the future fails with a {@link MemoizedFailureException} without invoking the loader.
   *
   * @param key the non-null key whose value should be loaded
   * @return the future value associated with the key
   */
  public CompletableFuture<V> getAsync(K key) {
    requireNonNull(key);
    FailureState state = failures.getIfPresent(key);
    Throwable error = (state == null) ? null : state.error;
    if (error != null) {
      failureHitCount.increment();
      return CompletableFuture.failedFuture(new MemoizedFailureException(error));
    }
    return cache.get(key);
  }

  /**
   * Returns the value associated with the key, loading it if necessary. If a memoized failure is
   * alive for the key then a {@link MemoizedFailureException} is thrown without invoking the
   * loader.
   *
   * @param key the non-null key whose value should be loaded
   * @return the value associated with the key
   * @throws MemoizedFailureException if a memoized failure is alive for the key
   */
  public V get(K key) {
    try {
      return getAsync(key).join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw e;
    }
  }

  /**
   * Discards the cached value and any memoized failure for the key. A failure from a load that
   * was started before this call, but completes afterwards, is discarded and cannot mask a value
   * loaded after this call.
   *
   * @param key the non-null key whose mapping should be removed
   */
  public void invalidate(K key) {
    requireNonNull(key);
    cache.synchronous().invalidate(key);
    synchronized (invalidationLock) {
      failures.asMap().put(key, FailureState.tombstone(tokens.incrementAndGet()));
    }
  }

  /** Discards all cached values and memoized failures, as described by {@link #invalidate}. */
  public void invalidateAll() {
    cache.synchronous().invalidateAll();
    synchronized (invalidationLock) {
      epoch.incrementAndGet();
      failures.invalidateAll();
    }
  }

  /** Returns the number of requests that were served a memoized failure. */
  public long failureHits() {
    return failureHitCount.sum();
  }

  /** Returns the number of load failures that were memoized. */
  public long memoizedFailures() {
    return memoizedFailureCount.sum();
  }

  /** Unwraps asynchronous execution wrappers to expose the underlying load failure. */
  static Throwable unwrap(Throwable error) {
    while (((error instanceof CompletionException) || (error instanceof ExecutionException))
        && (error.getCause() != null)) {
      error = error.getCause();
    }
    return error;
  }

  /** A memoized failure, or a tombstone marking that the key was explicitly invalidated. */
  static final class FailureState {
    final @Nullable Throwable error;
    final long token;

    FailureState(@Nullable Throwable error, long token) {
      this.error = error;
      this.token = token;
    }

    static FailureState failure(Throwable error, long token) {
      return new FailureState(error, token);
    }

    static FailureState tombstone(long token) {
      return new FailureState(null, token);
    }

    boolean isTombstone() {
      return error == null;
    }
  }

  /** Returns a builder that constructs {@link FailureMemoizingCache} instances. */
  public static <K, V> Builder<K, V> newBuilder() {
    return new Builder<>();
  }

  /** A builder of {@link FailureMemoizingCache} instances. */
  public static final class Builder<K, V> {
    @Nullable AsyncCacheLoader<? super K, V> loader;
    Predicate<Throwable> memoizable = error -> false;
    @Nullable Duration failureTtl;
    long maximumFailures = DEFAULT_MAXIMUM_FAILURES;
    Ticker ticker = Ticker.systemTicker();
    Executor executor = ForkJoinPool.commonPool();
    @Nullable Caffeine<? super K, ? super V> successCache;

    Builder() {}

    /** Sets the synchronous loader used to obtain values. */
    @CanIgnoreReturnValue
    public Builder<K, V> loader(CacheLoader<? super K, V> loader) {
      this.loader = requireNonNull(loader);
      return this;
    }

    /** Sets the asynchronous loader used to obtain values. */
    @CanIgnoreReturnValue
    public Builder<K, V> asyncLoader(AsyncCacheLoader<? super K, V> loader) {
      this.loader = requireNonNull(loader);
      return this;
    }

    /**
     * Sets the classifier that decides whether a load failure is memoized for the failure TTL.
     * Failures that are not accepted are never memoized and propagate to the caller. The default
     * classifier rejects all failures.
     */
    @CanIgnoreReturnValue
    public Builder<K, V> memoizable(Predicate<Throwable> memoizable) {
      this.memoizable = requireNonNull(memoizable);
      return this;
    }

    /** Sets how long a memoized failure is served before the loader is tried again. */
    @CanIgnoreReturnValue
    public Builder<K, V> failureTtl(Duration failureTtl) {
      this.failureTtl = requireNonNull(failureTtl);
      return this;
    }

    /**
     * Sets the maximum number of failure records held at a time. Records evicted due to this
     * bound cause the next request for the key to call the loader again. The default is
     * {@value #DEFAULT_MAXIMUM_FAILURES}.
     */
    @CanIgnoreReturnValue
    public Builder<K, V> maximumFailures(long maximumFailures) {
      requireArgument(maximumFailures > 0, "maximumFailures must be positive");
      this.maximumFailures = maximumFailures;
      return this;
    }

    /** Sets the time source used to expire memoized failures, primarily for testing. */
    @CanIgnoreReturnValue
    public Builder<K, V> ticker(Ticker ticker) {
      this.ticker = requireNonNull(ticker);
      return this;
    }

    /**
     * Sets the executor used when loading values. This is ignored if a
     * {@linkplain #successCache custom success cache} is supplied. The default is the
     * {@link ForkJoinPool#commonPool()}.
     */
    @CanIgnoreReturnValue
    public Builder<K, V> executor(Executor executor) {
      this.executor = requireNonNull(executor);
      return this;
    }

    /**
     * Sets the configuration of the cache holding successfully loaded values. By default an
     * unbounded cache is used.
     */
    @CanIgnoreReturnValue
    public Builder<K, V> successCache(Caffeine<? super K, ? super V> successCache) {
      this.successCache = requireNonNull(successCache);
      return this;
    }

    /** Returns a new {@link FailureMemoizingCache} with the requested configuration. */
    public FailureMemoizingCache<K, V> build() {
      requireNonNull(loader, "loader");
      requireNonNull(failureTtl, "failureTtl");
      return new FailureMemoizingCache<>(this);
    }
  }
}
