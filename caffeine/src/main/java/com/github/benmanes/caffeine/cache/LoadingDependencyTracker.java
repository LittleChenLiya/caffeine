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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import com.google.errorprone.annotations.Var;

/**
 * Tracks the in-flight loading dependency graph and detects cycles. A dependency is an edge from a
 * loading key to the key it is waiting on. Edges are removed promptly when a load completes or is
 * cancelled, so that only in-flight dependencies are tracked.
 * <p>
 * This class is thread-safe. All operations are synchronized on a single lock to ensure that the
 * cycle detection sees a consistent view of the graph.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the type of keys
 * @param <V> the type of values
 */
@NullMarked
final class LoadingDependencyTracker<K, V> {

  /** The in-flight loads, keyed by the loading key. */
  private final Map<K, LoadState> loads = new ConcurrentHashMap<>();

  /** The out-edges: key -> keys it depends on. */
  private final Map<K, Set<K>> dependencies = new ConcurrentHashMap<>();

  /** The in-edges: key -> keys that depend on it. */
  private final Map<K, Set<K>> dependents = new ConcurrentHashMap<>();

  /** Guards all graph mutations and cycle detection. */
  private final Object lock = new Object();

  /** The state of a single in-flight load. */
  final class LoadState {
    final K key;
    volatile @Nullable CompletableFuture<V> future;
    volatile boolean completed;

    LoadState(K key) {
      this.key = requireNonNull(key);
    }
  }

  /**
   * Notifies the tracker that a load for {@code key} is starting.
   *
   * @param key the loading key
   */
  void onLoadStart(K key) {
    requireNonNull(key);
    synchronized (lock) {
      loads.put(key, new LoadState(key));
    }
  }

  /**
   * Notifies the tracker of the future associated with the load for {@code key}. The tracker
   * registers a callback to clean up the load's edges when the future completes.
   *
   * @param key the loading key
   * @param future the load's future
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  void onLoadFuture(K key, CompletableFuture<V> future) {
    requireNonNull(key);
    requireNonNull(future);
    synchronized (lock) {
      LoadState state = loads.get(key);
      if ((state != null) && !state.completed) {
        state.future = future;
        future.whenComplete((v, e) -> onLoadComplete(key));
      }
    }
  }

  /**
   * Notifies the tracker that the load for {@code key} has completed or been cancelled. All edges
   * involving the key are removed.
   *
   * @param key the loading key
   */
  void onLoadComplete(K key) {
    requireNonNull(key);
    synchronized (lock) {
      LoadState state = loads.remove(key);
      if (state != null) {
        state.completed = true;
      }
      removeEdges(key);
    }
  }

  /**
   * Adds a dependency from {@code from} to {@code to}. If the dependency would form a cycle, the
   * key chain describing the cycle is returned and no edge is added.
   *
   * @param from the loading key
   * @param to the key being requested
   * @return the key chain if a cycle is detected, or {@code null} if the dependency was added
   */
  @Nullable List<K> addDependency(K from, K to) {
    requireNonNull(from);
    requireNonNull(to);
    synchronized (lock) {
      List<K> cycle = findCycle(from, to);
      if (cycle != null) {
        return cycle;
      }
      dependencies.computeIfAbsent(from, k -> ConcurrentHashMap.newKeySet()).add(to);
      dependents.computeIfAbsent(to, k -> ConcurrentHashMap.newKeySet()).add(from);
      return null;
    }
  }

  /**
   * Finds the cycle that would be formed by adding an edge from {@code from} to {@code to}. A cycle
   * exists if there is a path from {@code to} back to {@code from} following the dependency edges.
   * <p>
   * The returned chain starts with {@code from}, followed by the path from {@code to} to
   * {@code from}, and ends with {@code from}. For example, if {@code a} depends on {@code b} and
   * {@code b} depends on {@code a}, the chain is {@code [a, b, a]}.
   *
   * @param from the loading key
   * @param to the key being requested
   * @return the key chain if a cycle exists, or {@code null} if no cycle is formed
   */
  private @Nullable List<K> findCycle(K from, K to) {
    // BFS from 'to' following dependencies, see if we reach 'from'
    Map<K, @Nullable K> parent = new HashMap<>();
    Queue<K> queue = new ArrayDeque<>();
    Set<K> visited = new HashSet<>();

    queue.add(to);
    visited.add(to);
    parent.put(to, null);

    while (!queue.isEmpty()) {
      K current = requireNonNull(queue.poll());
      if (current.equals(from)) {
        // Found a path from 'to' to 'from'. Reconstruct the path.
        List<K> path = new ArrayList<>();
        @Var @Nullable K node = from;
        while (node != null) {
          path.add(node);
          node = parent.get(node);
        }
        Collections.reverse(path);
        // path is now [to, ..., from]
        // The cycle is [from, to, ..., from]
        List<K> cycle = new ArrayList<>();
        cycle.add(from);
        cycle.addAll(path);
        return cycle;
      }
      Set<K> deps = dependencies.get(current);
      if (deps != null) {
        for (K dep : deps) {
          if (!visited.contains(dep)) {
            visited.add(dep);
            parent.put(dep, current);
            queue.add(dep);
          }
        }
      }
    }
    return null;
  }

  /** Removes all edges involving {@code key}. */
  private void removeEdges(K key) {
    Set<K> deps = dependencies.remove(key);
    if (deps != null) {
      for (K dep : deps) {
        Set<K> in = dependents.get(dep);
        if (in != null) {
          in.remove(key);
        }
      }
    }
    Set<K> ins = dependents.remove(key);
    if (ins != null) {
      for (K inKey : ins) {
        Set<K> out = dependencies.get(inKey);
        if (out != null) {
          out.remove(key);
        }
      }
    }
  }

  /** Returns {@code true} if the tracker has any tracked edges. For testing only. */
  boolean hasEdges() {
    synchronized (lock) {
      return !dependencies.isEmpty() || !dependents.isEmpty();
    }
  }

  /** Returns the number of tracked edges. For testing only. */
  int edgeCount() {
    synchronized (lock) {
      @Var int count = 0;
      for (Set<K> deps : dependencies.values()) {
        count += deps.size();
      }
      return count;
    }
  }
}
