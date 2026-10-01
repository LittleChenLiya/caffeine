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

import com.google.errorprone.annotations.Immutable;

import org.jspecify.annotations.NullMarked;

/**
 * Statistics about the admission decisions of an {@link AsyncLoadAdmissionController}.
 * <p>
 * The statistics are incremented according to the following rules:
 * <ul>
 *   <li>Every call to {@link AsyncLoadAdmissionController#asyncLoad} increments
 *       {@code requestCount}.
 *   <li>A request for a key with an in-flight load, whether running or queued, joins that load
 *       and increments {@code coalescedCount} without consuming a slot or queue position.
 *   <li>A request admitted to a concurrency slot increments {@code startedCount} when the
 *       delegate loader is invoked, either immediately or when dequeued.
 *   <li>A request refused because the queue is full or the controller is closed increments
 *       {@code rejectedCount}.
 *   <li>A started load that completes successfully increments {@code loadSuccessCount}; one that
 *       completes exceptionally, throws, or is rejected by the executor increments
 *       {@code loadFailureCount}. In both cases the elapsed time since the load started, in
 *       nanoseconds, is added to {@code totalLoadTime}. Time spent waiting in the queue is not
 *       included.
 * </ul>
 * <p>
 * This is a <em>value-based</em> class; use of identity-sensitive operations (including reference
 * equality ({@code ==}), identity hash code, or synchronization) on instances of this class may
 * have unpredictable results and should be avoided.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@Immutable
@NullMarked
public final class LoadAdmissionStats {
  private final long requestCount;
  private final long coalescedCount;
  private final long startedCount;
  private final long rejectedCount;
  private final long loadSuccessCount;
  private final long loadFailureCount;
  private final long totalLoadTime;

  LoadAdmissionStats(long requestCount, long coalescedCount, long startedCount, long rejectedCount,
      long loadSuccessCount, long loadFailureCount, long totalLoadTime) {
    this.requestCount = requestCount;
    this.coalescedCount = coalescedCount;
    this.startedCount = startedCount;
    this.rejectedCount = rejectedCount;
    this.loadSuccessCount = loadSuccessCount;
    this.loadFailureCount = loadFailureCount;
    this.totalLoadTime = totalLoadTime;
  }

  /** Returns the number of times a load was requested. */
  public long requestCount() {
    return requestCount;
  }

  /** Returns the number of requests that joined an in-flight load of the same key. */
  public long coalescedCount() {
    return coalescedCount;
  }

  /** Returns the number of loads admitted to a concurrency slot and started. */
  public long startedCount() {
    return startedCount;
  }

  /** Returns the number of requests rejected with a {@link LoadRejectedException}. */
  public long rejectedCount() {
    return rejectedCount;
  }

  /** Returns the number of started loads that completed successfully. */
  public long loadSuccessCount() {
    return loadSuccessCount;
  }

  /** Returns the number of started loads that completed exceptionally. */
  public long loadFailureCount() {
    return loadFailureCount;
  }

  /** Returns the total time, in nanoseconds, spent by started loads, excluding queue waits. */
  public long totalLoadTime() {
    return totalLoadTime;
  }

  /** Returns the number of started loads that have completed, successfully or not. */
  public long completedLoadCount() {
    return loadSuccessCount + loadFailureCount;
  }

  /** Returns the average time, in nanoseconds, spent by a completed load, excluding queue waits. */
  public double averageLoadTime() {
    long completed = completedLoadCount();
    return (completed == 0) ? 0.0d : ((double) totalLoadTime / completed);
  }

  @Override public String toString() {
    return getClass().getSimpleName() + '{'
        + "requestCount=" + requestCount
        + ", coalescedCount=" + coalescedCount
        + ", startedCount=" + startedCount
        + ", rejectedCount=" + rejectedCount
        + ", loadSuccessCount=" + loadSuccessCount
        + ", loadFailureCount=" + loadFailureCount
        + ", totalLoadTime=" + totalLoadTime
        + '}';
  }
}
