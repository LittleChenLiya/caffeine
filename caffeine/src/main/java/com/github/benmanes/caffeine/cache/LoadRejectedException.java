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

import org.jspecify.annotations.NullMarked;

/**
 * An exception indicating that an {@link AsyncLoadAdmissionController} refused to admit a load.
 * A load is rejected when the controller is closed, or when all concurrency slots are in use and
 * the waiting queue is at capacity. The failed future is not cached, so a later request may be
 * admitted and retried.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NullMarked
public final class LoadRejectedException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /** Constructs a {@code LoadRejectedException} with the specified detail message. */
  public LoadRejectedException(String message) {
    super(message);
  }

  /** Constructs a {@code LoadRejectedException} with the specified detail message and cause. */
  public LoadRejectedException(String message, Throwable cause) {
    super(message, cause);
  }
}
