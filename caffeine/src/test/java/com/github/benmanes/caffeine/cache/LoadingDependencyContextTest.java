package com.github.benmanes.caffeine.cache;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.DependencyAwareAsyncCacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCycleException;
import com.github.benmanes.caffeine.cache.LoadingDependencyContext;
import com.github.benmanes.caffeine.cache.LocalAsyncLoadingCache;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
@SuppressWarnings({"CheckReturnValue", "FutureReturnValueIgnored"})
final class LoadingDependencyContextTest {

  /* --------------- Cycle detection --------------- */

  @Test
  void cycle_detected_when_a_waits_for_b_and_b_waits_for_a() {
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      if (key.equals("a")) {
        requireNonNull(context).get("b");
      } else {
        requireNonNull(context).get("a");
      }
      return CompletableFuture.completedFuture(key + "'s value");
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    // The cycle is detected synchronously when the loader for "b" calls context.get("a"),
    // which happens during the first cache.get("a") call (loaders run synchronously).
    LoadingCycleException exception = assertThrows(LoadingCycleException.class, () ->
        cache.get("a"));

    assertThat(exception.getChain()).containsExactly("b", "a", "b").inOrder();
  }

  @Test
  void cycle_detected_with_three_keys() {
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      if (key.equals("a")) {
        requireNonNull(context).get("b");
      } else if (key.equals("b")) {
        requireNonNull(context).get("c");
      } else {
        requireNonNull(context).get("a");
      }
      return CompletableFuture.completedFuture(key + "'s value");
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    // The cycle is detected synchronously when the loader for "c" calls context.get("a"),
    // which happens during the first cache.get("a") call (loaders run synchronously).
    LoadingCycleException exception = assertThrows(LoadingCycleException.class, () ->
        cache.get("a"));

    assertThat(exception.getChain()).containsExactly("c", "a", "b", "c").inOrder();
  }

  @Test
  void self_dependency_is_a_cycle() {
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      requireNonNull(context).get(key);
      return CompletableFuture.completedFuture(key + "'s value");
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    assertThrows(LoadingCycleException.class, () -> {
      try {
        cache.get("a").join();
      } catch (Exception e) {
        if (e.getCause() instanceof LoadingCycleException) {
          throw (LoadingCycleException) e.getCause();
        }
        throw e;
      }
    });
  }

  /* --------------- Independent loads --------------- */

  @Test
  void independent_loads_do_not_report_cycle() {
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) ->
        CompletableFuture.completedFuture(key + "'s value");

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    CompletableFuture<String> loadA = cache.get("a");
    CompletableFuture<String> loadB = cache.get("b");

    assertThat(loadA.join()).isEqualTo("a's value");
    assertThat(loadB.join()).isEqualTo("b's value");
  }

  @Test
  void two_independent_loads_of_same_key_do_not_report_cycle() {
    CompletableFuture<String> futureA = new CompletableFuture<>();
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> futureA;

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    CompletableFuture<String> load1 = cache.get("a");
    CompletableFuture<String> load2 = cache.get("a");

    futureA.complete("a's value");

    assertThat(load1.join()).isEqualTo("a's value");
    assertThat(load2.join()).isEqualTo("a's value");
  }

  /* --------------- Cleanup --------------- */

  @Test
  void edges_removed_when_load_completes() {
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      if (key.equals("a")) {
        requireNonNull(context).get("b");
      }
      return CompletableFuture.completedFuture(key + "'s value");
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    cache.get("a");
    cache.get("b");

    LocalAsyncLoadingCache<String, String> localCache =
        (LocalAsyncLoadingCache<String, String>) cache;
    assertThat(localCache.dependencyTracker.hasEdges()).isFalse();
    assertThat(localCache.dependencyTracker.edgeCount()).isEqualTo(0);
  }

  @Test
  void edges_removed_when_load_is_cancelled() {
    CompletableFuture<String> futureA = new CompletableFuture<>();
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> futureA;

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    CompletableFuture<String> loadA = cache.get("a");
    loadA.cancel(true);

    LocalAsyncLoadingCache<String, String> localCache =
        (LocalAsyncLoadingCache<String, String>) cache;
    assertThat(localCache.dependencyTracker.hasEdges()).isFalse();
  }

  /* --------------- Controllable Future --------------- */

  @Test
  void controllable_future_completes_and_cleans_up_edges() {
    CompletableFuture<String> futureA = new CompletableFuture<>();
    CompletableFuture<String> futureB = new CompletableFuture<>();

    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      if (key.equals("a")) {
        requireNonNull(context).get("b");
        return futureA;
      }
      return futureB;
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    cache.get("a");

    LocalAsyncLoadingCache<String, String> localCache =
        (LocalAsyncLoadingCache<String, String>) cache;
    assertThat(localCache.dependencyTracker.hasEdges()).isTrue();

    futureB.complete("b's value");
    futureA.complete("a's value");

    cache.get("a").join();

    assertThat(localCache.dependencyTracker.hasEdges()).isFalse();
  }

  /* --------------- Context usage --------------- */

  @Test
  void context_is_passed_to_loader() {
    AtomicReference<LoadingDependencyContext<String, String>> contextRef = new AtomicReference<>();
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      contextRef.set(requireNonNull(context));
      return CompletableFuture.completedFuture(key + "'s value");
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    cache.get("a").join();

    assertThat(contextRef.get()).isNotNull();
  }

  @Test
  void context_can_request_dependencies() {
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      if (key.equals("a")) {
        CompletableFuture<String> bFuture = requireNonNull(context).get("b");
        return bFuture.thenApply(b -> "a:" + b);
      }
      return CompletableFuture.completedFuture("b's value");
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    CompletableFuture<String> loadA = cache.get("a");

    assertThat(loadA.join()).isEqualTo("a:b's value");
  }

  /* --------------- Same Future shared --------------- */

  @Test
  void same_future_is_shared_between_loaders() {
    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      if (key.equals("a")) {
        requireNonNull(context).get("b");
      }
      return CompletableFuture.completedFuture(key + "'s value");
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    cache.get("a");
    cache.get("b");

    CompletableFuture<String> futureB1 = cache.getIfPresent("b");
    CompletableFuture<String> futureB2 = cache.getIfPresent("b");
    assertThat(futureB1).isSameInstanceAs(futureB2);
  }

  /* --------------- Diagnostics do not cancel others' loads --------------- */

  @Test
  void cycle_detection_does_not_cancel_other_futures() {
    CompletableFuture<String> futureA = new CompletableFuture<>();
    CompletableFuture<String> futureB = new CompletableFuture<>();

    DependencyAwareAsyncCacheLoader<String, String> loader = (key, executor, context) -> {
      if (key.equals("a")) {
        requireNonNull(context).get("b");
        return futureA;
      } else {
        requireNonNull(context).get("a");
        return futureB;
      }
    };

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    // The cycle is detected synchronously when the loader for "b" calls context.get("a"),
    // which happens during the first cache.get("a") call (loaders run synchronously).
    assertThrows(LoadingCycleException.class, () -> cache.get("a"));

    assertThat(futureA.isCancelled()).isFalse();
    assertThat(futureB.isCancelled()).isFalse();
  }

  /* --------------- Old loader interface still works --------------- */

  @Test
  void old_loader_interface_still_works() {
    AsyncCacheLoader<String, String> loader = (key, executor) ->
        CompletableFuture.completedFuture(key + "'s value");

    AsyncLoadingCache<String, String> cache = Caffeine.newBuilder().buildAsync(loader);

    CompletableFuture<String> loadA = cache.get("a");

    assertThat(loadA.join()).isEqualTo("a's value");
  }
}
