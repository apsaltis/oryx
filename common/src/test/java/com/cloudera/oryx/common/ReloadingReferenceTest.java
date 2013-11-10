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

package com.cloudera.oryx.common;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link ReloadingReference}.
 *
 * @author Sean Owen
 */
public final class ReloadingReferenceTest extends OryxTest {

  @Test
  public void testNoReload() {
    Callable<Number> callable = new Callable<Number>() {
      private final AtomicInteger callCounts = new AtomicInteger();
      @Override
      public Number call() throws Exception {
        return callCounts.incrementAndGet();
      }
    };
    ReloadingReference<Number> reload = new ReloadingReference<Number>(callable);
    assertNull(reload.maybeGet());
    assertEquals(1, reload.get());
    assertEquals(1, reload.get());
    reload.clear();
    assertEquals(2, reload.get());
  }

  @Test
  public void testReload() throws InterruptedException {
    Callable<Number> callable = new Callable<Number>() {
      private final AtomicInteger callCounts = new AtomicInteger();
      @Override
      public Number call() throws Exception {
        return callCounts.incrementAndGet();
      }
    };
    ReloadingReference<Number> reload = new ReloadingReference<Number>(callable, 500, TimeUnit.MILLISECONDS);
    assertNull(reload.maybeGet());
    assertEquals(1, reload.get());
    assertEquals(1, reload.get());
    Thread.sleep(1000L);
    assertEquals(2, reload.get());
  }

}
