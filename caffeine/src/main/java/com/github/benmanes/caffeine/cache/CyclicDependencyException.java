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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.jspecify.annotations.NullMarked;

/**
 * An exception that indicates that a loading dependency was rejected because it would have
 * introduced a cycle into the graph of in-flight loads. The dependency is refused before any
 * waiting begins, so the requester fails explicitly instead of deadlocking, and the loads
 * already in flight are left running for their other callers.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @see LoadDependencyTracker
 */
@NullMarked
public final class CyclicDependencyException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("serial") // the key chain is copied into an immutable, serializable list
  private final List<?> keyChain;

  /**
   * Constructs an exception describing the cyclic dependency.
   *
   * @param keyChain the chain of keys forming the cycle, where each key waits on the next and
   *     the last key equals the first
   */
  public CyclicDependencyException(List<?> keyChain) {
    super("Cyclic loading dependency: " + keyChain.stream()
        .map(String::valueOf)
        .collect(Collectors.joining(" -> ")));
    this.keyChain = Collections.unmodifiableList(new ArrayList<>(keyChain));
  }

  /**
   * Returns the chain of keys forming the rejected cycle, in waiting order. The first key is
   * the in-flight load that was requested, each subsequent key waits on its predecessor, and
   * the last key repeats the first to close the cycle (for example {@code [a, b, a]} when the
   * load of {@code a} waits on {@code b} and the load of {@code b} requests {@code a}).
   *
   * @return the chain of keys forming the cycle
   */
  public List<?> keyChain() {
    return keyChain;
  }
}
