/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.common.parallel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Utility methods related to {@link ExecutorService} and related classes.
 *
 * @author Sean Owen
 */
public final class ExecutorUtils {

  private static final Logger log = LoggerFactory.getLogger(ExecutorUtils.class);

  private ExecutorUtils() {
  }

  /**
   * @return parallelism that should be used across the project when computation is parallelized
   */
  public static int getParallelism() {
    Config config = ConfigUtils.getDefaultConfig();
    if (config.hasPath("computation-layer.parallelism")) {
      String parallelismString = config.getString("computation-layer.parallelism");
      if ("auto".equals(parallelismString)) {
        return Runtime.getRuntime().availableProcessors();
      } else {
        return Integer.parseInt(parallelismString);
      }
    }
    return Runtime.getRuntime().availableProcessors();
  }

  /**
   * Immediately shuts down its argument and waits a short time for it to terminate.
   */
  public static void shutdownNowAndAwait(ExecutorService executor) {
    if (!executor.isTerminated()) {
      if (!executor.isShutdown()) {
        executor.shutdownNow();
      }
      await(executor);
    }
  }

  /**
   * Shuts down its argument, letting tasks finish, and waits a short time for it to terminate.
   */
  public static void shutdownAndAwait(ExecutorService executor) {
    if (!executor.isTerminated()) {
      if (!executor.isShutdown()) {
        executor.shutdown();
      }
      await(executor);
    }
  }

  private static void await(ExecutorService executor) {
    try {
      executor.awaitTermination(5L, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
      log.warn("Interrupted while shutting down executor");
    }
  }

  /**
   * Checks results for exceptions by calling all {@link Future#get()}.
   *
   * @throws IllegalStateException if any generated an exception
   */
  public static <T> void checkExceptions(Iterable<Future<T>> futures) {
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(e.getCause());
      }
    }
  }

}
