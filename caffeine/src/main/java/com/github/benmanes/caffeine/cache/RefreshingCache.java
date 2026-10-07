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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * A standalone stale-while-revalidate cache with a hard staleness cap. Each entry is in one of
 * three states, measured from its last successful load using the configured {@link Ticker}:
 * <ul>
 *   <li><b>Fresh</b> (age &lt; fresh duration): the cached value is returned and no load occurs.
 *   <li><b>Stale</b> (age &lt; fresh + stale durations): the cached value is returned and a
 *       single background refresh is started if none is in flight.
 *   <li><b>Expired</b> (older): the cached value is no longer served; the caller waits on the
 *       shared in-flight load, or observes the load failure.
 * </ul>
 * At most one load per key is in flight at any time and its result is shared by all waiting
 * callers. A failed load leaves the previous value untouched so that stale reads may continue to
 * be served within the staleness cap, while expired reads observe the failure and retry on the
 * next access. An entry invalidated while its refresh is in flight discards the refresh result,
 * so a stale or invalidated value is never resurrected.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NullMarked
public final class RefreshingCache<K, V> {
  private final ConcurrentMap<K, Entry<V>> map;
  private final AsyncCacheLoader<? super K, V> loader;
  private final long freshNanos;
  private final long staleNanos;
  private final Ticker ticker;
  private final Executor executor;

  private RefreshingCache(Builder builder, AsyncCacheLoader<? super K, V> loader) {
    this.map = new ConcurrentHashMap<>();
    this.loader = requireNonNull(loader);
    this.freshNanos = builder.freshDuration.toNanos();
    this.staleNanos = builder.staleDuration.toNanos();
    this.ticker = builder.ticker;
    this.executor = builder.executor;
  }

  /** Returns a builder that constructs a {@link RefreshingCache}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns the future value associated with {@code key}. A fresh or stale cached value is
   * returned as an already-completed future, with a stale value additionally triggering a
   * background refresh. If the entry is absent or older than the staleness cap, the returned
   * future is the shared in-flight load and completes with either the loaded value or the load
   * failure.
   *
   * @param key the non-null key whose value should be returned
   * @return the future value associated with the key
   */
  public CompletableFuture<V> get(K key) {
    requireNonNull(key);
    long now = ticker.read();
    Entry<V> entry = map.computeIfAbsent(key, k -> new Entry<>());
    synchronized (entry) {
      V value = entry.value;
      if (value != null) {
        long age = now - entry.writeTime;
        if (age < freshNanos) {
          return CompletableFuture.completedFuture(value);
        } else if (age < freshNanos + staleNanos) {
          startLoadLocked(key, entry);
          return CompletableFuture.completedFuture(value);
        }
      }
      return startLoadLocked(key, entry);
    }
  }

  /**
   * Returns the cached value associated with {@code key}, or {@code null} if there is none. This
   * method does not trigger a load regardless of the entry freshness.
   *
   * @param key the non-null key whose value should be returned
   * @return the cached value, or {@code null} if none is cached
   */
  public @Nullable V getIfPresent(K key) {
    Entry<V> entry = map.get(requireNonNull(key));
    if (entry == null) {
      return null;
    }
    synchronized (entry) {
      return entry.value;
    }
  }

  /**
   * Discards the cached value for {@code key}. If a load is in flight its result is discarded
   * when it completes, so the invalidated value is not resurrected.
   *
   * @param key the non-null key whose mapping should be removed
   */
  public void invalidate(K key) {
    map.remove(requireNonNull(key));
  }

  /** Discards all cached values. */
  public void invalidateAll() {
    map.clear();
  }

  /** Returns the in-flight load for the entry, starting one if none is in flight. */
  @CanIgnoreReturnValue
  private CompletableFuture<V> startLoadLocked(K key, Entry<V> entry) {
    CompletableFuture<V> inFlight = entry.refresh;
    if (inFlight != null) {
      return inFlight;
    }
    CompletableFuture<? extends V> loaded;
    try {
      loaded = requireNonNull(loader.asyncLoad(key, executor));
    } catch (Throwable t) {
      return CompletableFuture.failedFuture(t);
    }
    var future = new CompletableFuture<V>();
    var unusedLoaded = loaded.whenComplete((value, error) -> {
      if (error == null) {
        future.complete(value);
      } else {
        future.completeExceptionally(error);
      }
    });
    entry.refresh = future;
    var unusedCommit = future.whenComplete((value, error) -> {
      synchronized (entry) {
        if (entry.refresh == future) {
          entry.refresh = null;
        }
        if ((error == null) && (value != null) && (map.get(key) == entry)) {
          entry.value = value;
          entry.writeTime = ticker.read();
        }
      }
    });
    return future;
  }

  /** The per-key state, guarded by the entry monitor. */
  private static final class Entry<V> {
    @Nullable V value;
    long writeTime;
    @Nullable CompletableFuture<V> refresh;
  }

  /** A builder of {@link RefreshingCache} instances. */
  public static final class Builder {
    private Duration freshDuration = Duration.ZERO;
    private Duration staleDuration = Duration.ZERO;
    private Ticker ticker = Ticker.systemTicker();
    private Executor executor = ForkJoinPool.commonPool();

    private Builder() {}

    /** Sets the duration for which a loaded value is served without triggering a refresh. */
    @CanIgnoreReturnValue
    public Builder freshFor(Duration duration) {
      this.freshDuration = requireNonNull(duration);
      return this;
    }

    /** Sets the maximum duration a stale value may be served beyond the fresh duration. */
    @CanIgnoreReturnValue
    public Builder staleFor(Duration duration) {
      this.staleDuration = requireNonNull(duration);
      return this;
    }

    /** Sets the time source used to determine freshness. */
    @CanIgnoreReturnValue
    public Builder ticker(Ticker ticker) {
      this.ticker = requireNonNull(ticker);
      return this;
    }

    /** Sets the executor with which asynchronous loads are performed. */
    @CanIgnoreReturnValue
    public Builder executor(Executor executor) {
      this.executor = requireNonNull(executor);
      return this;
    }

    /** Returns a cache that loads values using the given {@code loader}. */
    public <K, V> RefreshingCache<K, V> build(AsyncCacheLoader<? super K, V> loader) {
      if (freshDuration.isNegative() || staleDuration.isNegative()) {
        throw new IllegalArgumentException("Durations must not be negative");
      }
      return new RefreshingCache<>(this, loader);
    }
  }
}
