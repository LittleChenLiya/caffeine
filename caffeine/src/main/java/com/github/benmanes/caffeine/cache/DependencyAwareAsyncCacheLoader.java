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
import java.util.concurrent.Executor;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * An {@link AsyncCacheLoader} that is provided a {@link LoadingDependencyContext} so that it can
 * request other keys while loading. The context tracks the in-flight dependency graph and rejects a
 * request that would form a cycle.
 * <p>
 * Loaders that do not need to request other keys should implement {@link AsyncCacheLoader} instead.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the type of keys
 * @param <V> the type of values
 */
@NullMarked
@FunctionalInterface
public interface DependencyAwareAsyncCacheLoader<K, V extends @Nullable Object>
    extends AsyncCacheLoader<K, V> {

  /**
   * Asynchronously computes or retrieves the value corresponding to {@code key}.
   * <p>
   * <b>Warning:</b> loading <b>must not</b> attempt to update any mappings of this cache directly.
   *
   * @param key the non-null key whose value should be loaded
   * @param executor the executor with which the entry may be asynchronously loaded
   * @param context the loading dependency context, used to request other keys
   * @return the future value associated with {@code key}
   * @throws Exception or Error, in which case the mapping is unchanged
   * @throws InterruptedException if this method is interrupted. {@code InterruptedException} is
   *         treated like any other {@code Exception} in all respects except that, when it is
   *         caught, the thread's interrupt status is set
   */
  CompletableFuture<? extends V> asyncLoad(
      K key, Executor executor, @Nullable LoadingDependencyContext<K, V> context) throws Exception;

  /**
   * Asynchronously computes or retrieves the value corresponding to {@code key}, without a
   * dependency context. This method delegates to the three-argument {@code asyncLoad} method with a
   * {@code null} context.
   *
   * @param key the non-null key whose value should be loaded
   * @param executor the executor with which the entry may be asynchronously loaded
   * @return the future value associated with {@code key}
   * @throws Exception or Error, in which case the mapping is unchanged
   */
  @Override
  default CompletableFuture<? extends V> asyncLoad(K key, Executor executor) throws Exception {
    return asyncLoad(key, executor, null);
  }
}
