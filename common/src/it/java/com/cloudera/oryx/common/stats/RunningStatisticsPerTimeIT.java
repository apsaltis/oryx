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

package com.cloudera.oryx.common.stats;

import java.util.concurrent.TimeUnit;

import com.cloudera.oryx.common.OryxTest;
import org.junit.Test;

/**
 * Tests {@link RunningStatisticsPerTime}.
 *
 * @author Sean Owen
 */
public final class RunningStatisticsPerTimeIT extends OryxTest {

  @Test
  public void testInit() {
    RunningStatisticsPerTime perTime = new RunningStatisticsPerTime(TimeUnit.MINUTES);
    assertTrue(Double.isNaN(perTime.getMin()));
    assertTrue(Double.isNaN(perTime.getMax()));
    assertTrue(Double.isNaN(perTime.getMean()));
    assertEquals(0L, perTime.getCount());
  }

  @Test
  public void testOneBucket() {
    RunningStatisticsPerTime perTime = new RunningStatisticsPerTime(TimeUnit.MINUTES);
    perTime.increment(1.2);
    assertEquals(1.2, perTime.getMin());
    assertEquals(1.2, perTime.getMax());
    assertEquals(1.2, perTime.getMean());
    assertEquals(1L, perTime.getCount());
  }

}
