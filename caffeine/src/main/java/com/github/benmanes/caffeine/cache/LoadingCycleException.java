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

import java.util.List;

import org.jspecify.annotations.NullMarked;

/**
 * Thrown when a loading dependency cycle is detected. The {@linkplain #getChain() key chain}
 * describes the cycle that was found, starting and ending with the same key.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NullMarked
@SuppressWarnings("serial")
public final class LoadingCycleException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final transient List<?> chain;

  /**
   * Creates a new instance with the key chain that formed the cycle.
   *
   * @param chain the keys that form the cycle, starting and ending with the same key
   */
  public LoadingCycleException(List<?> chain) {
    super("Loading dependency cycle detected: " + chain);
    this.chain = chain;
  }

  /**
   * Returns the keys that form the cycle, starting and ending with the same key.
   *
   * @return the key chain
   */
  @SuppressWarnings("unchecked")
  public List<Object> getChain() {
    return (List<Object>) chain;
  }
}
