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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.google.errorprone.annotations.Var;

/**
 * An example of {@link AsyncLoadAdmissionController} that prints the concurrency slots and the
 * load order as four loads flow through two slots and a queue of capacity two, with a fifth
 * load rejected. Runs in well under 90 seconds.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class AsyncLoadAdmissionExampleTest {

  @Test
  @SuppressWarnings("SystemOut")
  void demonstratesSlotsAndLoadOrder() throws Exception {
    var events = new ConcurrentLinkedQueue<String>();
    long origin = System.nanoTime();
    var executor = Executors.newCachedThreadPool();

    AsyncCacheLoader<String, String> loader = (key, asyncExecutor) -> {
      events.add(event(origin, "START  " + key));
      return CompletableFuture.supplyAsync(() -> {
        sleep(100);
        events.add(event(origin, "FINISH " + key));
        return "value-" + key;
      }, asyncExecutor);
    };

    var admission = AsyncLoadAdmissionController.create(loader, 2, 2);
    var keys = List.of("a", "b", "c", "d");
    var futures = new ArrayList<CompletableFuture<String>>();
    for (String key : keys) {
      futures.add(admission.asyncLoad(key, executor));
    }

    // A fifth distinct key overflows the queue and is rejected immediately.
    var rejected = admission.asyncLoad("e", executor);
    events.add(event(origin, "REJECT e (queue full)"));

    var reporter = new Thread(() -> report(admission, events, origin));
    reporter.start();

    var results = new ArrayList<String>();
    for (var future : futures) {
      results.add(future.get(90, TimeUnit.SECONDS));
    }
    reporter.join();
    executor.shutdown();

    events.forEach(System.out::println);
    System.out.println("results: " + results);
    System.out.println("stats:   " + admission.stats());

    assertThat(results).containsExactly("value-a", "value-b", "value-c", "value-d").inOrder();
    assertThat(rejected.isCompletedExceptionally()).isTrue();
    assertThat(admission.stats().startedCount()).isEqualTo(4);
    assertThat(admission.stats().rejectedCount()).isEqualTo(1);
    assertThat(startOrder(events)).containsExactly("a", "b", "c", "d").inOrder();
  }

  /** Prints the slot usage every 25ms until all loads have drained. */
  private static void report(AsyncLoadAdmissionController<String, String> admission,
      ConcurrentLinkedQueue<String> events, long origin) {
    @Var int quietPeriods = 0;
    while (quietPeriods < 4) {
      int running = admission.runningCount();
      int queued = admission.queuedCount();
      quietPeriods = ((running == 0) && (queued == 0)) ? (quietPeriods + 1) : 0;
      events.add(event(origin, "slots  running=" + running + " queued=" + queued));
      sleep(25);
    }
  }

  private static List<String> startOrder(ConcurrentLinkedQueue<String> events) {
    var order = new ArrayList<String>();
    for (String event : events) {
      if (event.contains("START")) {
        order.add(event.substring(event.length() - 1));
      }
    }
    return order;
  }

  private static String event(long origin, String description) {
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - origin);
    return String.format(Locale.ROOT, "[%4dms] %s", elapsedMillis, description);
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
