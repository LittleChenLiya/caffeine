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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * The test cases for {@link LoadDependencyTracker}, driven by controllable futures so that the
 * interleavings of in-flight loads are deterministic.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class LoadDependencyTrackerTest {
  static final Executor DIRECT_EXECUTOR = Runnable::run;
  static final long JOIN_TIMEOUT_MILLIS = 10_000;

  @Test
  void context_exposesKeyAndExecutor() {
    var loader = new ControlledLoader();
    var tracker = new LoadDependencyTracker<String, String>(loader);

    var result = tracker.asyncLoad("a", DIRECT_EXECUTOR);
    var context = loader.context("a");
    assertThat(context.key()).isEqualTo("a");
    assertThat(context.executor()).isSameInstanceAs(DIRECT_EXECUTOR);

    result.cancel(/* mayInterruptIfRunning= */ true);
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void independentLoads_sameKey_shareUnderlyingFuture() {
    var loader = new ControlledLoader();
    var tracker = new LoadDependencyTracker<String, String>(loader);

    var first = tracker.asyncLoad("a", DIRECT_EXECUTOR);
    var second = tracker.asyncLoad("a", DIRECT_EXECUTOR);

    // two independent loads of the same key share one load, and are not a cycle
    assertThat(first).isSameInstanceAs(second);
    assertThat(loader.invocations.get()).isEqualTo(1);

    loader.future("a").complete("A");
    assertThat(first.join()).isEqualTo("A");
    assertThat(second.join()).isEqualTo("A");
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void independentLoads_concurrent_noFalseCycle() throws Exception {
    var loader = new ControlledLoader();
    var tracker = new LoadDependencyTracker<String, String>(loader);
    var futureA = new AtomicReference<CompletableFuture<String>>();
    var futureB = new AtomicReference<CompletableFuture<String>>();

    var threadA = new Thread(() -> futureA.set(tracker.asyncLoad("a", DIRECT_EXECUTOR)));
    var threadB = new Thread(() -> futureB.set(tracker.asyncLoad("b", DIRECT_EXECUTOR)));
    threadA.start();
    threadB.start();
    threadA.join(JOIN_TIMEOUT_MILLIS);
    threadB.join(JOIN_TIMEOUT_MILLIS);
    assertThat(threadA.isAlive()).isFalse();
    assertThat(threadB.isAlive()).isFalse();

    // independent loads of different keys are in flight together without a cycle
    assertThat(futureA.get().isDone()).isFalse();
    assertThat(futureB.get().isDone()).isFalse();
    assertThat(tracker.isIdle()).isFalse();

    loader.future("a").complete("A");
    loader.future("b").complete("B");
    assertThat(futureA.get().join()).isEqualTo("A");
    assertThat(futureB.get().join()).isEqualTo("B");
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void independentLoads_concurrentSameKey_shareWithoutCycle() throws Exception {
    var loader = new ControlledLoader();
    var tracker = new LoadDependencyTracker<String, String>(loader);
    var first = new AtomicReference<CompletableFuture<String>>();
    var second = new AtomicReference<CompletableFuture<String>>();

    var thread1 = new Thread(() -> first.set(tracker.asyncLoad("a", DIRECT_EXECUTOR)));
    var thread2 = new Thread(() -> second.set(tracker.asyncLoad("a", DIRECT_EXECUTOR)));
    thread1.start();
    thread2.start();
    thread1.join(JOIN_TIMEOUT_MILLIS);
    thread2.join(JOIN_TIMEOUT_MILLIS);

    assertThat(first.get()).isSameInstanceAs(second.get());
    assertThat(loader.invocations.get()).isEqualTo(1);

    loader.future("a").complete("A");
    assertThat(first.get().join()).isEqualTo("A");
    assertThat(second.get().join()).isEqualTo("A");
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void diamondDependencies_noCycle() {
    ContextualAsyncCacheLoader<String, String> loader = (key, context) -> {
      if (key.equals("a")) {
        return context.get("b").thenCombine(context.get("c"), (left, right) -> left + right);
      } else if (key.equals("b")) {
        return context.get("c").thenApply(value -> value + "b");
      }
      return CompletableFuture.completedFuture(key);
    };
    var tracker = new LoadDependencyTracker<>(loader);

    assertThat(tracker.asyncLoad("a", DIRECT_EXECUTOR).join()).isEqualTo("cbc");
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void selfDependency_rejected() {
    ContextualAsyncCacheLoader<String, String> loader = (key, context) ->
        context.get(key).thenApply(value -> value + "!");
    var tracker = new LoadDependencyTracker<>(loader);

    var cycle = assertCyclic(tracker.asyncLoad("a", DIRECT_EXECUTOR));
    assertThat(cycle.keyChain()).containsExactly("a", "a").inOrder();
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void cyclicDependency_rejected() {
    var loader = new ControlledLoader();
    var tracker = new LoadDependencyTracker<String, String>(loader);

    var resultA = tracker.asyncLoad("a", DIRECT_EXECUTOR);
    var resultB = tracker.asyncLoad("b", DIRECT_EXECUTOR);

    // a waits on b: the load of b is already in flight and is shared
    var dependencyOfA = loader.context("a").get("b");
    assertThat(dependencyOfA.isDone()).isFalse();
    assertThat(loader.invocations.get()).isEqualTo(2);

    // b waiting on a would close the cycle a -> b -> a, so the dependency is refused
    var cycle = assertCyclic(loader.context("b").get("a"));
    assertThat(cycle.keyChain()).containsExactly("a", "b", "a").inOrder();

    // the diagnosis does not cancel the in-flight loads owned by other callers
    assertThat(resultA.isDone()).isFalse();
    assertThat(resultB.isDone()).isFalse();
    assertThat(dependencyOfA.isDone()).isFalse();

    // completing the loads clears every dependency edge
    loader.future("b").complete("B");
    assertThat(dependencyOfA.join()).isEqualTo("B");
    assertThat(resultB.join()).isEqualTo("B");
    loader.future("a").complete("A");
    assertThat(resultA.join()).isEqualTo("A");
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void cyclicDependency_loaderChain_failsWithoutHanging() {
    ContextualAsyncCacheLoader<String, String> loader = (key, context) -> {
      var other = key.equals("a") ? "b" : "a";
      return context.get(other).thenApply(value -> value + key);
    };
    var tracker = new LoadDependencyTracker<>(loader);

    var cycle = assertCyclic(tracker.asyncLoad("a", DIRECT_EXECUTOR));
    assertThat(cycle.keyChain()).containsExactly("a", "b", "a").inOrder();
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void cyclicDependency_concurrent_failsWithoutHanging() throws Exception {
    ContextualAsyncCacheLoader<String, String> loader = (key, context) -> {
      var other = key.equals("a") ? "b" : "a";
      return context.get(other).thenApply(value -> value + key);
    };
    var tracker = new LoadDependencyTracker<>(loader);
    var futureA = new AtomicReference<CompletableFuture<String>>();
    var futureB = new AtomicReference<CompletableFuture<String>>();

    var threadA = new Thread(() -> futureA.set(tracker.asyncLoad("a", DIRECT_EXECUTOR)));
    var threadB = new Thread(() -> futureB.set(tracker.asyncLoad("b", DIRECT_EXECUTOR)));
    threadA.start();
    threadB.start();
    threadA.join(JOIN_TIMEOUT_MILLIS);
    threadB.join(JOIN_TIMEOUT_MILLIS);

    // both loads fail explicitly with the cycle instead of deadlocking
    assertThat(threadA.isAlive()).isFalse();
    assertThat(threadB.isAlive()).isFalse();
    var cycleA = assertCyclic(futureA.get());
    var cycleB = assertCyclic(futureB.get());
    assertThat(cycleA.keyChain().get(0)).isEqualTo(cycleA.keyChain().get(2));
    assertThat(cycleB.keyChain().get(0)).isEqualTo(cycleB.keyChain().get(2));
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void cancellation_discardsInFlightLoad() {
    var loader = new ControlledLoader();
    var tracker = new LoadDependencyTracker<String, String>(loader);

    var result = tracker.asyncLoad("a", DIRECT_EXECUTOR);
    assertThat(tracker.isIdle()).isFalse();

    result.cancel(/* mayInterruptIfRunning= */ true);
    assertThat(tracker.isIdle()).isTrue();

    // a cancelled load leaves no edges behind, so the key can be loaded again
    var retry = tracker.asyncLoad("a", DIRECT_EXECUTOR);
    assertThat(retry.isDone()).isFalse();
    loader.future("a").complete("A");
    assertThat(retry.join()).isEqualTo("A");
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void dependencyView_cancellationDoesNotCancelSharedLoad() {
    var loader = new ControlledLoader();
    var tracker = new LoadDependencyTracker<String, String>(loader);

    var resultA = tracker.asyncLoad("a", DIRECT_EXECUTOR);
    var dependency = loader.context("a").get("b");
    var direct = tracker.asyncLoad("b", DIRECT_EXECUTOR);

    // abandoning the dependency does not cancel the load shared with other callers
    dependency.cancel(/* mayInterruptIfRunning= */ true);
    assertThat(direct.isDone()).isFalse();
    assertThat(tracker.isIdle()).isFalse();

    loader.future("b").complete("B");
    assertThat(direct.join()).isEqualTo("B");

    loader.future("a").complete("A");
    assertThat(resultA.join()).isEqualTo("A");
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void completedLoad_discardsEdgesAndAllowsReload() {
    var loader = new ControlledLoader();
    var tracker = new LoadDependencyTracker<String, String>(loader);

    var first = tracker.asyncLoad("a", DIRECT_EXECUTOR);
    var dependency = loader.context("a").get("b");
    loader.future("b").complete("B");
    assertThat(dependency.join()).isEqualTo("B");
    loader.future("a").complete("A");
    assertThat(first.join()).isEqualTo("A");
    assertThat(tracker.isIdle()).isTrue();

    // the completed round left no edges behind, so the reversed dependency is not a cycle
    var resultB = tracker.asyncLoad("b", DIRECT_EXECUTOR);
    var dependencyOfB = loader.context("b").get("a");
    assertThat(dependencyOfB.isDone()).isFalse();
    loader.future("a").complete("A2");
    assertThat(dependencyOfB.join()).isEqualTo("A2");
    loader.future("b").complete("B2");
    assertThat(resultB.join()).isEqualTo("B2");
    assertThat(tracker.isIdle()).isTrue();
  }

  @Test
  void asyncLoadingCache_loadsThroughContext() {
    ContextualAsyncCacheLoader<String, String> loader = (key, context) -> key.equals("a")
        ? context.get("b").thenApply(value -> value + "a")
        : CompletableFuture.completedFuture(key);
    AsyncLoadingCache<String, String> cache =
        Caffeine.newBuilder().buildAsync(new LoadDependencyTracker<>(loader));

    assertThat(cache.get("a").join()).isEqualTo("ba");
    assertThat(cache.synchronous().get("a")).isEqualTo("ba");
  }

  @Test
  void asyncLoadingCache_cycle_failsWithoutHanging() {
    ContextualAsyncCacheLoader<String, String> loader = (key, context) -> {
      var other = key.equals("a") ? "b" : "a";
      return context.get(other).thenApply(value -> value + key);
    };
    var tracker = new LoadDependencyTracker<>(loader);
    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(tracker);

    var cycle = assertCyclic(cache.get("a"));
    assertThat(cycle.keyChain()).containsExactly("a", "b", "a").inOrder();
    assertThat(tracker.isIdle()).isTrue();
  }

  /** Asserts that the future fails with a {@link CyclicDependencyException} and returns it. */
  static CyclicDependencyException assertCyclic(CompletableFuture<?> future) {
    var error = assertThrows(CompletionException.class, future::join);
    var cycle = findCyclic(error);
    assertThat(cycle).isNotNull();
    return cycle;
  }

  /** Returns the first {@link CyclicDependencyException} in the causal chain, if any. */
  static @Nullable CyclicDependencyException findCyclic(@Nullable Throwable error) {
    for (@Nullable Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof CyclicDependencyException) {
        return (CyclicDependencyException) cause;
      }
    }
    return null;
  }

  /** A loader whose loads are completed manually by the test. */
  static final class ControlledLoader implements ContextualAsyncCacheLoader<String, String> {
    final Map<String, LoadingDependencyContext<String, String>> contexts =
        new ConcurrentHashMap<>();
    final Map<String, CompletableFuture<String>> futures = new ConcurrentHashMap<>();
    final AtomicInteger invocations = new AtomicInteger();

    @Override
    public CompletableFuture<String> asyncLoad(
        String key, LoadingDependencyContext<String, String> context) {
      contexts.put(key, context);
      invocations.incrementAndGet();
      var future = new CompletableFuture<String>();
      futures.put(key, future);
      return future;
    }

    /** Returns the context that the load of {@code key} was supplied with. */
    LoadingDependencyContext<String, String> context(String key) {
      return requireNonNull(contexts.get(key));
    }

    /** Returns the controllable future that the load of {@code key} returned. */
    CompletableFuture<String> future(String key) {
      return requireNonNull(futures.get(key));
    }
  }
}
