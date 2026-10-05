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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.jspecify.annotations.Nullable;
import org.jspecify.annotations.NullMarked;

/**
 * An {@link AsyncCacheLoader} that delegates to a {@link ContextualAsyncCacheLoader} and
 * tracks the dependencies between in-flight loads. While a load is in flight, the loader may
 * request other keys through its {@link LoadingDependencyContext}; each request is recorded as
 * a dependency edge and refused with a {@link CyclicDependencyException} if it would close a
 * cycle of loads waiting on each other. Only in-flight loads are tracked: an edge is discarded
 * as soon as either of its loads completes or is cancelled, so independent loads of the same
 * key are never mistaken for a cycle, and a key can be loaded again once its previous load has
 * finished.
 * <p>
 * A key has at most one load in flight at a time. Concurrent requests for the same key share
 * the same underlying future, and a dependency request returns a view of that future so that
 * abandoning one dependency cannot cancel a load owned by other callers. When a cycle is
 * detected, only the new dependency is rejected; the in-flight loads forming the cycle are
 * left running for their owners.
 * <p>
 * Usage example:
 * {@snippet class=com.github.benmanes.caffeine.cache.Snippets
 *           region=loadDependencyTracker lang=java}
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NullMarked
public final class LoadDependencyTracker<K, V> implements AsyncCacheLoader<K, V> {
  private final ContextualAsyncCacheLoader<K, V> loader;
  private final Map<K, Load<K, V>> inFlight;
  private final Object lock;

  /**
   * Constructs a tracker that delegates to the supplied loader.
   *
   * @param loader the loader to delegate to, supplied with a {@link LoadingDependencyContext}
   */
  public LoadDependencyTracker(ContextualAsyncCacheLoader<K, V> loader) {
    this.loader = requireNonNull(loader);
    this.inFlight = new HashMap<>();
    this.lock = new Object();
  }

  /**
   * Starts a load for {@code key}, or shares the load already in flight. This adapts the
   * {@link ContextualAsyncCacheLoader} to the {@link AsyncCacheLoader} contract, so a tracker
   * can be supplied directly to {@link Caffeine#buildAsync(AsyncCacheLoader)}.
   */
  @Override
  public CompletableFuture<V> asyncLoad(K key, Executor executor) {
    return request(/* requester= */ null, key, executor);
  }

  /** Returns whether no loads are in flight and no dependency edges remain. */
  public boolean isIdle() {
    synchronized (lock) {
      return inFlight.isEmpty();
    }
  }

  /**
   * Returns a future of the value for {@code key}, recording a dependency edge from
   * {@code requester} when it is non-null. If the edge would close a cycle of in-flight
   * dependencies, the request is refused with a {@link CyclicDependencyException}.
   */
  CompletableFuture<V> request(@Nullable Load<K, V> requester, K key, Executor executor) {
    requireNonNull(key);
    requireNonNull(executor);

    Load<K, V> load;
    boolean started;
    synchronized (lock) {
      load = inFlight.get(key);
      if (load == null) {
        load = new Load<>(key);
        inFlight.put(key, load);
        started = true;
      } else {
        started = false;
        if (requester != null) {
          var cycle = findPath(load, requester);
          if (cycle != null) {
            var keyChain = new ArrayList<Object>(cycle.size() + 1);
            cycle.forEach(node -> keyChain.add(node.key()));
            keyChain.add(load.key());
            return CompletableFuture.failedFuture(new CyclicDependencyException(keyChain));
          }
        }
      }
      if (requester != null) {
        requester.addDependency(load);
      }
    }

    if (started) {
      var startedLoad = load;
      startedLoad.future().whenComplete((value, error) -> discard(startedLoad));
      start(startedLoad, executor);
    }
    // A dependency receives a view so that abandoning it cannot cancel the shared load, while
    // a top-level caller owns the shared future and may cancel it to end the load promptly.
    return (requester == null) ? load.future() : dependentView(load.future());
  }

  /** Starts the load by invoking the delegate loader with a new context. */
  private void start(Load<K, V> load, Executor executor) {
    CompletableFuture<? extends V> loaded;
    try {
      var context = new LoadingDependencyContext<>(this, load, executor);
      loaded = requireNonNull(loader.asyncLoad(load.key(), context), "null future");
    } catch (Throwable t) {
      load.future().completeExceptionally(t);
      return;
    }
    loaded.whenComplete((value, error) -> {
      if (error == null) {
        load.future().complete(value);
      } else {
        load.future().completeExceptionally(error);
      }
    });
  }

  /** Discards the load and every dependency edge that references it. */
  private void discard(Load<K, V> load) {
    synchronized (lock) {
      inFlight.remove(load.key(), load);
      load.clearDependencies();
      for (var other : inFlight.values()) {
        other.removeDependency(load);
      }
    }
  }

  /**
   * Returns the path of loads from {@code from} to {@code to} following dependency edges, or
   * null if {@code to} is not reachable. A non-null result means that a dependency from
   * {@code to} on {@code from} would close a cycle.
   */
  private static <K, V> @Nullable List<Load<K, V>> findPath(Load<K, V> from, Load<K, V> to) {
    if (from == to) {
      return List.of(from);
    }
    var parent = new IdentityHashMap<Load<K, V>, Load<K, V>>();
    var queue = new ArrayDeque<Load<K, V>>();
    queue.add(from);
    while (!queue.isEmpty()) {
      var node = queue.remove();
      for (var dependency : node.dependencies()) {
        if ((dependency == from) || parent.containsKey(dependency)) {
          continue;
        }
        parent.put(dependency, node);
        if (dependency == to) {
          var path = new ArrayList<Load<K, V>>();
          for (@Nullable Load<K, V> cursor = to; cursor != null; cursor = parent.get(cursor)) {
            path.add(cursor);
          }
          Collections.reverse(path);
          return path;
        }
        queue.add(dependency);
      }
    }
    return null;
  }

  /** Returns a future that mirrors the source, but whose cancellation does not propagate. */
  private static <V> CompletableFuture<V> dependentView(CompletableFuture<V> future) {
    return future.copy();
  }

  /** An in-flight load and the loads it waits on, guarded by the lock of the tracker. */
  static final class Load<K, V> {
    private final CompletableFuture<V> future = new CompletableFuture<>();
    private final Set<Load<K, V>> dependencies = new HashSet<>();
    private final K key;

    Load(K key) {
      this.key = requireNonNull(key);
    }
    K key() {
      return key;
    }
    CompletableFuture<V> future() {
      return future;
    }
    Set<Load<K, V>> dependencies() {
      return dependencies;
    }
    void addDependency(Load<K, V> load) {
      dependencies.add(load);
    }
    void removeDependency(Load<K, V> load) {
      dependencies.remove(load);
    }
    void clearDependencies() {
      dependencies.clear();
    }
  }
}
