/**
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

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.math.Vectors;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

public final class ClusterValidityStatisticsTest extends OryxTest {

  private final RealVector a = Vectors.of(17.0, 30.0);
  private final RealVector b = Vectors.of(18.0, 27.0);
  private final RealVector c = Vectors.of(16.0, 22.0);

  @Test
  public void testSingleton() throws Exception {
    Iterable<WeightedRealVector> pts = ImmutableList.of(
        new WeightedRealVector(a, 10.0),
        new WeightedRealVector(b, 20.0),
        new WeightedRealVector(c, 40.0));
    Centers c1 = new Centers(Vectors.of(16.0, 22.0));
    ClusterValidityStatistics cvs = ClusterValidityStatistics.create(pts, c1, c1, 0);
    assertNaN(cvs.getVanDongen());
    assertNaN(cvs.getVariationOfInformation());
  }

  @Test
  public void testIdentical() throws Exception {
    Iterable<WeightedRealVector> pts = ImmutableList.of(
        new WeightedRealVector(a, 10.0),
        new WeightedRealVector(b, 20.0),
        new WeightedRealVector(c, 40.0));
    Centers c1 = new Centers(Vectors.of(16.0, 26.0), Vectors.of(17.5, 28.0));
    ClusterValidityStatistics cvs = ClusterValidityStatistics.create(pts, c1, c1, 0);
    assertEquals(707.5, cvs.getTestCost(), 0.001);
    assertEquals(707.5, cvs.getTrainCost(), 0.001);
    assertEquals(0.0, cvs.getVanDongen());
    assertEquals(0.0, cvs.getVariationOfInformation());
  }

  @Test
  public void testReordered() throws Exception {
    Iterable<WeightedRealVector> pts = ImmutableList.of(
        new WeightedRealVector(a, 10.0),
        new WeightedRealVector(b, 20.0),
        new WeightedRealVector(c, 40.0));
    Centers c1 = new Centers(Vectors.of(16.0, 26.0), Vectors.of(17.5, 28.0));
    Centers c2 = new Centers(Vectors.of(17.5, 28.0), Vectors.of(16.0, 26.0));
    ClusterValidityStatistics cvs = ClusterValidityStatistics.create(pts, c1, c2, 0);
    assertEquals(707.5, cvs.getTestCost(), 0.001);
    assertEquals(707.5, cvs.getTrainCost(), 0.001);
    assertEquals(0.0, cvs.getVanDongen());
    assertEquals(0.0, cvs.getVariationOfInformation());
  }

  @Test
  public void testDifferent() throws Exception {
    Iterable<WeightedRealVector> pts = ImmutableList.of(
        new WeightedRealVector(a, 10.0),
        new WeightedRealVector(b, 20.0),
        new WeightedRealVector(c, 40.0));
    Centers c1 = new Centers(Vectors.of(16.0, 22.0), Vectors.of(18.0, 27.0));
    Centers c2 = new Centers(Vectors.of(17.0, 30.0), Vectors.of(17.1, 29.9));
    ClusterValidityStatistics cvs = ClusterValidityStatistics.create(pts, c1, c2, 0);
    assertEquals(100.0, cvs.getTestCost(), 0.001);
    assertEquals(2729.2, cvs.getTrainCost(), 0.001);
    assertEquals(0.75, cvs.getVanDongen());
    assertEquals(0.748, cvs.getVariationOfInformation(), 0.001);
  }

  @Test
  public void testDifferentReweighted() throws Exception {
    Iterable<WeightedRealVector> pts = ImmutableList.of(
        new WeightedRealVector(a, 19.0),
        new WeightedRealVector(b, 7.0),
        new WeightedRealVector(c, 11.0));
    Centers c1 = new Centers(Vectors.of(16.0, 22.0), Vectors.of(18.0, 27.0));
    Centers c2 = new Centers(Vectors.of(17.0, 30.0), Vectors.of(17.1, 29.9));
    ClusterValidityStatistics cvs = ClusterValidityStatistics.create(pts, c1, c2, 0);
    assertEquals(190.0, cvs.getTestCost(), 0.001);
    assertEquals(764.36, cvs.getTrainCost(), 0.001);
    assertEquals(0.483, cvs.getVanDongen(), 0.001);
    assertEquals(0.564, cvs.getVariationOfInformation(), 0.001);
  }
}
