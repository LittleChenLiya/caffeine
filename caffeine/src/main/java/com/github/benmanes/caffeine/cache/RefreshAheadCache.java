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
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;

/*
 * A refresh policy with two independently configured windows, measured from when an entry was
 * written. A read within the fresh window returns the value directly; a read within the stale
 * window returns the old value and schedules a single background reload shared by every concurrent
 * reader; a read past the stale window joins that reload. An entry is a single monotonic sequence
 * of loads guarded by its monitor, and invalidate ends the sequence: a load that completes after
 * its entry was removed finds nothing to populate, so it cannot resurrect the mapping.
 */

/**
 * A refresh-ahead cache that serves stale values while a reload runs in the background.
 * <p>
 * Two durations are configured independently and measured against an injected {@link Ticker}:
 * <ul>
 *   <li>Before {@code refreshAfterWrite} elapses the entry is <i>fresh</i> and a read returns its
 *       value without loading.
 *   <li>After the fresh window but before {@code refreshAfterWrite + staleAfterRefresh} elapses
 *       the entry is <i>stale</i>: a read returns the old value immediately and schedules a
 *       background reload. The reload is coalesced, so at most one load per key runs at a time and
 *       every concurrent read observes the same one.
 *   <li>Past that age the old value may no longer be served: a read waits for the shared load and
 *       receives its new value, or the load failure is propagated to the caller.
 * </ul>
 * <p>
 * A missing key is always loaded before a value is returned. A load that returns {@code null}
 * caches nothing and removes the mapping (including a stale value it refreshed), and its waiters
 * receive {@code null}. A failed background refresh keeps the prior value; subsequent stale reads
 * retry it, and once the value is past the stale window reads wait on each attempt and receive the
 * failure. An {@link #invalidate} while a load runs cancels the in-flight waiters; the late load
 * result is discarded and cannot repopulate the mapping.
 * <p>
 * Exceptions thrown by a loader are reported by {@link #get} wrapped in a
 * {@link CompletionException} whose cause is the loader's exception; {@link Error}s are rethrown
 * directly.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NullMarked
public final class RefreshAheadCache<K, V> {
  private final AsyncCacheLoader<K, V> loader;
  private final long refreshAfterWriteNanos;
  private final long maxStalenessNanos;
  private final Map<K, Entry<V>> entries;
  private final Executor executor;
  private final Ticker ticker;

  RefreshAheadCache(AsyncCacheLoader<K, V> loader, Executor executor, Ticker ticker,
      long refreshAfterWriteNanos, long staleAfterRefreshNanos) {
    this.refreshAfterWriteNanos = refreshAfterWriteNanos;
    this.maxStalenessNanos = refreshAfterWriteNanos + staleAfterRefreshNanos;
    this.entries = new ConcurrentHashMap<>();
    this.executor = executor;
    this.loader = loader;
    this.ticker = ticker;
  }

  /**
   * Creates a new builder.
   *
   * @param <K> the type of keys
   * @param <V> the type of values
   * @return a new builder
   */
  public static <K, V> Builder<K, V> newBuilder() {
    return new Builder<>();
  }

  /**
   * Returns the value associated with {@code key}, loading it if absent.
   *
   * @param key the non-null key whose value to retrieve
   * @return the value, or {@code null} if the loader returns {@code null}
   * @throws CompletionException if a waited-on load fails; its cause is the loader exception
   * @throws CancellationException if the entry is invalidated while this call waits
   */
  public @Nullable V get(K key) {
    requireNonNull(key);
    for (;;) {
      var entry = entries.computeIfAbsent(key, unused -> new Entry<>());
      CompletableFuture<V> load;
      @Nullable V staleValue;
      boolean dispatch;
      synchronized (entry) {
        if (entry.removed) {
          // Invalidated between computeIfAbsent and the lock; retry with the winner.
          continue;
        }
        long age = ticker.read() - entry.writeTimeNanos;
        if (entry.pendingLoad != null) {
          if ((entry.value != null) && (age < maxStalenessNanos)) {
            // A refresh is already running and the old value is still within the stale window.
            return entry.value;
          }
          load = entry.pendingLoad;
          staleValue = null;
          dispatch = false;
        } else if ((entry.value != null) && (age < refreshAfterWriteNanos)) {
          return entry.value;
        } else {
          load = new CompletableFuture<>();
          entry.pendingLoad = load;
          staleValue = ((entry.value != null) && (age < maxStalenessNanos))
              ? entry.value
              : null;
          dispatch = true;
        }
      }

      // Invoke the loader only after releasing the entry monitor: a future the loader returns
      // already complete runs its completion callback on this thread, which re-enters the monitor.
      if (dispatch) {
        dispatch(key, entry, load);
      }
      if (staleValue != null) {
        return staleValue;
      }
      try {
        return load.join();
      } catch (CompletionException e) {
        var cause = e.getCause();
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw e;
      }
    }
  }

  /** Invokes the loader outside the entry monitor and relays its outcome to {@code load}. */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void dispatch(K key, Entry<V> entry, CompletableFuture<V> load) {
    CompletableFuture<? extends V> loaderFuture;
    try {
      loaderFuture = requireNonNull(loader.asyncLoad(key, executor));
    } catch (Throwable t) {
      complete(key, entry, load, null, unwrap(t));
      return;
    }
    loaderFuture.whenComplete(
        (value, error) -> complete(key, entry, load, value, unwrap(error)));
  }

  /**
   * Applies a load outcome to the load's own entry, then releases its waiters. An outcome
   * belonging to a removed or superseded entry is discarded.
   */
  private void complete(K key, Entry<V> entry, CompletableFuture<V> load,
      @Nullable V value, @Nullable Throwable error) {
    @Var boolean remove = false;
    synchronized (entry) {
      if (entry.removed || (entry.pendingLoad != load)) {
        return;
      }
      entry.pendingLoad = null;
      if (error != null) {
        // A failed refresh retains the old value; a failed first load drops the entry.
        remove = (entry.value == null);
      } else if (value == null) {
        // The loader declared the mapping absent; a null value is never retained.
        remove = true;
      } else {
        entry.value = value;
        entry.writeTimeNanos = ticker.read();
      }
      if (remove) {
        entry.removed = true;
        entries.remove(key, entry);
      }
    }
    if (error != null) {
      load.completeExceptionally(error);
    } else {
      completeNullably(load, value);
    }
  }

  /** Completes {@code load} with a possibly null value; NullAway cannot see through the future. */
  @SuppressWarnings("NullAway")
  private static <V> void completeNullably(CompletableFuture<V> load, @Nullable V value) {
    load.complete(value);
  }

  /**
   * Discards the mapping for {@code key}, if any. Waiters on a load in flight are cancelled; the
   * late load result is discarded.
   *
   * @param key the non-null key whose mapping is discarded
   */
  public void invalidate(K key) {
    requireNonNull(key);
    var entry = entries.get(key);
    if (entry == null) {
      return;
    }
    @Nullable CompletableFuture<V> pending;
    synchronized (entry) {
      if (entry.removed) {
        return;
      }
      entry.removed = true;
      pending = entry.pendingLoad;
      entries.remove(key, entry);
    }
    if (pending != null) {
      pending.cancel(false);
    }
  }

  /** Wraps a throwable from the loader in a CompletionException, unless it is one or an Error. */
  private static @Nullable Throwable unwrap(@Nullable Throwable t) {
    if ((t == null) || (t instanceof CompletionException) || (t instanceof Error)) {
      return t;
    }
    return new CompletionException(t);
  }

  /** The per-key state; its monitor guards every field. */
  private static final class Entry<V> {
    /** The last successfully loaded value, or null until the first load completes. */
    @Nullable V value;

    /** Ticker time at which value was written; zero while the first load is in flight. */
    long writeTimeNanos;

    /**
     * The outstanding load shared by stale-triggering and waiting reads, or null when idle. It is
     * the future readers join, so invalidating the mapping cancels its waiters directly, and a
     * late completion is dropped because the entry no longer references it.
     */
    @Nullable CompletableFuture<V> pendingLoad;

    /** Whether invalidate ended this entry's load sequence. */
    boolean removed;
  }

  /**
   * A builder of {@link RefreshAheadCache} instances.
   *
   * @param <K> the type of keys
   * @param <V> the type of values
   */
  public static final class Builder<K, V> {
    private @Nullable AsyncCacheLoader<K, V> loader;
    private @Nullable Executor executor;
    private @Nullable Ticker ticker;
    private long refreshAfterWriteNanos;
    private long staleAfterRefreshNanos;

    Builder() {}

    /**
     * Specifies the age at which an entry stops being fresh and reads begin serving it stale while
     * refreshing in the background.
     *
     * @param duration the fresh window
     * @return this builder
     */
    @CanIgnoreReturnValue
    public Builder<K, V> refreshAfterWrite(Duration duration) {
      this.refreshAfterWriteNanos = requirePositive(duration);
      return this;
    }

    /**
     * Specifies how long a stale value may still be served after the fresh window ends. After this
     * additional duration reads wait for the load.
     *
     * @param duration the maximum staleness window
     * @return this builder
     */
    @CanIgnoreReturnValue
    public Builder<K, V> staleAfterRefresh(Duration duration) {
      this.staleAfterRefreshNanos = requirePositive(duration);
      return this;
    }

    /**
     * Specifies the time source used to determine freshness and staleness.
     *
     * @param ticker the ticker
     * @return this builder
     */
    @CanIgnoreReturnValue
    public Builder<K, V> ticker(Ticker ticker) {
      this.ticker = requireNonNull(ticker);
      return this;
    }

    /**
     * Specifies the executor handed to the loader.
     *
     * @param executor the executor
     * @return this builder
     */
    @CanIgnoreReturnValue
    public Builder<K, V> executor(Executor executor) {
      this.executor = requireNonNull(executor);
      return this;
    }

    /**
     * Specifies the loader used to compute values.
     *
     * @param loader the cache loader
     * @return this builder
     */
    @CanIgnoreReturnValue
    public Builder<K, V> loader(AsyncCacheLoader<K, V> loader) {
      this.loader = requireNonNull(loader);
      return this;
    }

    /**
     * Builds the cache.
     *
     * @return a new cache
     * @throws IllegalStateException if a required setting is missing or inconsistent
     */
    public RefreshAheadCache<K, V> build() {
      if (loader == null) {
        throw new IllegalStateException("loader was not set");
      } else if (refreshAfterWriteNanos <= 0) {
        throw new IllegalStateException("refreshAfterWrite was not set");
      } else if (staleAfterRefreshNanos <= 0) {
        throw new IllegalStateException("staleAfterRefresh was not set");
      }
      var resolvedTicker = (ticker == null) ? Ticker.systemTicker() : ticker;
      var resolvedExecutor = (executor == null) ? ForkJoinPool.commonPool() : executor;
      return new RefreshAheadCache<>(loader, resolvedExecutor, resolvedTicker,
          refreshAfterWriteNanos, staleAfterRefreshNanos);
    }

    private static long requirePositive(Duration duration) {
      long nanos = duration.toNanos();
      if (nanos <= 0) {
        throw new IllegalArgumentException("duration must be positive: " + duration);
      }
      return nanos;
    }
  }
}
