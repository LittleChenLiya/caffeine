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

import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.NullMarked;

/**
 * A context provided to a {@link DependencyAwareAsyncCacheLoader} so that it can request other keys
 * while loading. The context tracks the in-flight dependency graph and rejects a request that would
 * form a cycle.
 * <p>
 * A new context is created for each load and is only valid for the duration of that load. The
 * context is safe to use from the loader's thread, including when the load is performed
 * asynchronously.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the type of keys
 * @param <V> the type of values
 */
@NullMarked
public interface LoadingDependencyContext<K, V> {

  /**
   * Requests the value associated with {@code key}, registering a dependency from the current load
   * to {@code key}. If the request would form a cycle, a {@link LoadingCycleException} is thrown
   * with the key chain that describes the cycle.
   * <p>
   * The returned future is shared with any other loaders that request the same key. The dependency
   * is tracked only while both the current load and the requested load are in-flight.
   *
   * @param key the non-null key whose value should be loaded
   * @return the future value associated with {@code key}
   * @throws LoadingCycleException if the request would form a loading dependency cycle
   */
  CompletableFuture<V> get(K key);
}
