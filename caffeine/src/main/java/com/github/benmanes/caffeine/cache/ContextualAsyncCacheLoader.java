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
 * Computes or retrieves a value asynchronously based on a key, where the load may request
 * additional keys through a {@link LoadingDependencyContext}. Unlike {@link AsyncCacheLoader},
 * the loader receives an explicit context that tracks the dependencies between in-flight loads
 * so that a circular wait is rejected with a {@link CyclicDependencyException} instead of
 * deadlocking.
 * <p>
 * This interface is used with a {@link LoadDependencyTracker}, which adapts it to the
 * {@link AsyncCacheLoader} contract expected by {@link Caffeine#buildAsync(AsyncCacheLoader)}.
 * Loaders that do not request other keys should continue to use {@link AsyncCacheLoader}
 * directly.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 * @see LoadDependencyTracker
 */
@NullMarked
@FunctionalInterface
@SuppressWarnings("PMD.SignatureDeclareThrowsException")
public interface ContextualAsyncCacheLoader<K, V> {

  /**
   * Asynchronously computes or retrieves the value corresponding to {@code key}. The load may
   * request the values of other keys through {@link LoadingDependencyContext#get}; each such
   * request is recorded as a dependency of this load for as long as it remains in flight.
   *
   * @param key the non-null key whose value should be loaded
   * @param context the loading dependency context for requesting other keys
   * @return the future value associated with {@code key}
   * @throws Exception or Error, in which case the load is treated as failed
   */
  CompletableFuture<? extends V> asyncLoad(K key, LoadingDependencyContext<K, V> context)
      throws Exception;
}
