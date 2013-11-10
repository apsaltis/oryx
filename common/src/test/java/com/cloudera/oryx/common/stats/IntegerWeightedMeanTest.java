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

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link IntegerWeightedMean}.
 *
 * @author Sean Owen
 */
public final class IntegerWeightedMeanTest extends OryxTest {

  @Test
  public void testNone() {
    IntegerWeightedMean mean = new IntegerWeightedMean();
    assertEquals(0, mean.getN());
    assertTrue(Double.isNaN(mean.getResult()));
  }

  @Test
  public void testOne() {
    IntegerWeightedMean mean = new IntegerWeightedMean();
    mean.increment(1.5);
    assertEquals(1, mean.getN());
    assertEquals(1.5, mean.getResult());
  }

  @Test
  public void testWeighted() {
    IntegerWeightedMean mean = new IntegerWeightedMean();
    mean.increment(0.2, 4);
    mean.increment(-0.1, 2);
    assertEquals(2, mean.getN());
    assertEquals(0.1, mean.getResult());
  }

  @Test
  public void testNegative() {
    IntegerWeightedMean mean = new IntegerWeightedMean();
    mean.increment(-0.1, 2);
    mean.increment(0.1, 2);
    assertEquals(2, mean.getN());
    assertEquals(0.0, mean.getResult());
  }

  @Test
  public void testComplex() {
    IntegerWeightedMean mean = new IntegerWeightedMean();
    for (int i = 1; i <= 5; i++) {
      mean.increment(1.0 / (i + 1), i);
    }
    assertEquals(5, mean.getN());
    assertEquals((1.0 / 2.0 + 2.0 / 3.0 + 3.0 / 4.0 + 4.0 / 5.0 + 5.0 / 6.0) / 15.0, mean.getResult());
  }

  @Test
  public void testDecrement() {
    IntegerWeightedMean mean = new IntegerWeightedMean();
    for (int i = 1; i <= 5; i++) {
      mean.increment(1.0 / (i + 1), i);
    }
    mean.decrement(1.0 / 6.0, 5);
    mean.decrement(1.0 / 5.0, 4);
    assertEquals(3, mean.getN());
    assertEquals((1.0 / 2.0 + 2.0 / 3.0 + 3.0 / 4.0) / 6.0, mean.getResult());
    mean.increment(1.0 / 6.0, 5);
    mean.increment(1.0 / 5.0, 4);
    assertEquals(5, mean.getN());
    assertEquals((1.0 / 2.0 + 2.0 / 3.0 + 3.0 / 4.0 + 4.0 / 5.0 + 5.0 / 6.0) / 15.0, mean.getResult());
  }

}
