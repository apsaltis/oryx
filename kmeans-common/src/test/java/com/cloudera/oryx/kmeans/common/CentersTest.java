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

package com.cloudera.oryx.kmeans.common;

import com.cloudera.oryx.common.math.Vectors;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class CentersTest extends OryxTest {
  private static final double THRESH = 0.001;
  
  private final RealVector a = Vectors.of(17.0, 29.0);
  private final RealVector b = Vectors.of(18.0, 27.0);
  private final RealVector c = Vectors.of(16.0, 25.0);
  
  @Test
  public void testSingleton() throws Exception {
    Centers centers = new Centers(a);
    assertEquals(0.0, centers.getDistance(a).getSquaredDistance(), THRESH);
    assertEquals(5.0, centers.getDistance(b).getSquaredDistance(), THRESH);
    assertEquals(17.0, centers.getDistance(c).getSquaredDistance(), THRESH);
    assertEquals(0, centers.getDistance(b).getClosestCenterId());
  }
  
  @Test
  public void testTwo() throws Exception {
    Centers centers = new Centers(a, b);
    assertEquals(0.0, centers.getDistance(a).getSquaredDistance(), THRESH);
    assertEquals(0.0, centers.getDistance(b).getSquaredDistance(), THRESH);
    assertEquals(8.0, centers.getDistance(c).getSquaredDistance(), THRESH);
    assertEquals(1, centers.getDistance(c).getClosestCenterId());
    assertEquals(0, centers.getDistance(a).getClosestCenterId());
  }
}
