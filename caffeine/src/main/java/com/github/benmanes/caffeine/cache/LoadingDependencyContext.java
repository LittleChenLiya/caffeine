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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.jspecify.annotations.NullMarked;

/**
 * The loading dependency context for a single in-flight load, through which a
 * {@link ContextualAsyncCacheLoader} requests the values of other keys. Each request is
 * recorded as a dependency edge from this load to the load of the requested key, and the edge
 * is discarded as soon as either load completes or is cancelled. If a request would close a
 * cycle of in-flight dependencies, it is rejected with a {@link CyclicDependencyException}
 * rather than being allowed to deadlock.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 * @see LoadDependencyTracker
 */
@NullMarked
public final class LoadingDependencyContext<K, V> {
  private final LoadDependencyTracker<K, V> tracker;
  private final LoadDependencyTracker.Load<K, V> load;
  private final Executor executor;

  LoadingDependencyContext(
      LoadDependencyTracker<K, V> tracker, LoadDependencyTracker.Load<K, V> load,
      Executor executor) {
    this.executor = requireNonNull(executor);
    this.tracker = requireNonNull(tracker);
    this.load = requireNonNull(load);
  }

  /** Returns the key whose load this context belongs to. */
  public K key() {
    return load.key();
  }

  /** Returns the executor that the load was initiated with. */
  public Executor executor() {
    return executor;
  }

  /**
   * Requests the value associated with {@code dependencyKey}, starting a new load if none is
   * in flight and otherwise sharing the in-flight load. The returned future is a view of the
   * shared load: cancelling it abandons only this dependency, and does not cancel the load
   * that other callers may still be waiting on.
   * <p>
   * If this load is already (transitively) waited on by the load of {@code dependencyKey},
   * then the request is refused and the returned future fails with a
   * {@link CyclicDependencyException} detailing the key chain. The in-flight loads forming
   * the cycle are not cancelled, as they may be owned by other callers.
   *
   * @param dependencyKey the non-null key whose value this load depends on
   * @return a future of the value associated with {@code dependencyKey}
   */
  public CompletableFuture<V> get(K dependencyKey) {
    return tracker.request(load, dependencyKey, executor);
  }
}
