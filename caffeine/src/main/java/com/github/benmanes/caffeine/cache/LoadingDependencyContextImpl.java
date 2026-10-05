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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.NullMarked;

/**
 * The implementation of {@link LoadingDependencyContext}. A new instance is created for each load
 * and is only valid for the duration of that load.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the type of keys
 * @param <V> the type of values
 */
@NullMarked
final class LoadingDependencyContextImpl<K, V> implements LoadingDependencyContext<K, V> {

  final K currentKey;
  final LoadingDependencyTracker<K, V> tracker;
  final LocalAsyncLoadingCache<K, V> cache;

  LoadingDependencyContextImpl(
      K currentKey, LoadingDependencyTracker<K, V> tracker, LocalAsyncLoadingCache<K, V> cache) {
    this.currentKey = requireNonNull(currentKey);
    this.tracker = requireNonNull(tracker);
    this.cache = requireNonNull(cache);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    requireNonNull(key);
    List<K> cycle = tracker.addDependency(currentKey, key);
    if (cycle != null) {
      @SuppressWarnings({"rawtypes", "unchecked"})
      var chain = (List<Object>) (List) cycle;
      throw new LoadingCycleException(chain);
    }
    return cache.get(key);
  }
}
