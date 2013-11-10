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

import com.cloudera.oryx.common.OryxTest;
import org.junit.Test;

/**
 * Tests {@link RunningStatistics}.
 *
 * @author Sean Owen
 */
public final class RunningStatisticsTest extends OryxTest {

  @Test
  public void testInstantiate() {
    RunningStatistics stats = new RunningStatistics();
    assertNaN(stats.getMin());
    assertNaN(stats.getMax());
    stats.increment(Integer.MIN_VALUE);
    assertEquals(Integer.MIN_VALUE, stats.getMin());
    assertEquals(Integer.MIN_VALUE, stats.getMax());
    stats.increment(Integer.MAX_VALUE);
    assertEquals(Integer.MIN_VALUE, stats.getMin());
    assertEquals(Integer.MAX_VALUE, stats.getMax());
  }

}
