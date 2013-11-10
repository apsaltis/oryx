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

import java.util.List;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.random.RandomManager;

import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;

import com.cloudera.oryx.common.math.Vectors;
import com.google.common.collect.ImmutableList;

public final class KMeansTest extends OryxTest {

  private final Weighted<RealVector> a = wpoint(1.0, 1.0);
  private final Weighted<RealVector> b = wpoint(5.0, 4.0);
  private final Weighted<RealVector> c = wpoint(4.0, 3.0);
  private final Weighted<RealVector> d = wpoint(2.0, 1.0);
  private final List<Weighted<RealVector>> points = ImmutableList.of(a, b, c, d);
  private final List<Weighted<RealVector>> degeneratePoints = ImmutableList.of(a, a, a, a);
  private final KMeansUpdateStrategy lloyds = new LloydsUpdateStrategy(10);
  
  private static Weighted<RealVector> wpoint(double... values) {
    return new Weighted<RealVector>(Vectors.of(values));
  }

  @Test
  public void testCentroids() throws Exception {
    assertEquals(Vectors.of(3.0, 2.5), LloydsUpdateStrategy.centroid(ImmutableList.of(a, b)));
    assertEquals(Vectors.of(3.0, 2.0), LloydsUpdateStrategy.centroid(ImmutableList.of(c, d)));
    assertEquals(Vectors.of(1.0, 1.0), LloydsUpdateStrategy.centroid(ImmutableList.of(a)));
  }
  
  @Test
  public void testUpdate() throws Exception {
    Centers centers = new Centers(a.thing(), b.thing());
    Centers expected = new Centers(Vectors.of(1.5, 1.0), Vectors.of(4.5, 3.5));
    assertEquals(expected, lloyds.update(points, centers));
  }
  
  @Test
  public void testConvergence() throws Exception {
    Centers centers = new Centers(a.thing(), b.thing());
    Centers converged = lloyds.update(points, centers);
    Centers expected = new Centers(Vectors.of(1.5, 1.0), Vectors.of(4.5, 3.5));
    assertEquals(expected, converged);
  }

  @Test
  public void testRandomInit() throws Exception {
    Centers expected = new Centers(Vectors.of(4.0, 3.0), Vectors.of(1.0, 1.0));
    assertEquals(expected, KMeansInitStrategy.RANDOM.apply(points, 2, RandomManager.getRandom()));
    
    Centers done = lloyds.update(points, expected);
    assertEquals(new Centers(Vectors.of(4.5, 3.5), Vectors.of(1.5, 1.0)), done);
  }
  
  @Test
  public void testPlusPlusInit() throws Exception {
    Centers expected = new Centers(Vectors.of(1.0, 1.0), Vectors.of(5.0, 4.0));
    assertEquals(expected, KMeansInitStrategy.PLUS_PLUS.apply(points, 2, RandomManager.getRandom()));
    
    Centers done = lloyds.update(points, expected);
    assertEquals(new Centers(Vectors.of(1.5, 1.0), Vectors.of(4.5, 3.5)), done);
  }
  
  @Test
  public void testMiniBatch() throws Exception {
    Centers centers = new Centers(Vectors.of(2.0, 1.0), Vectors.of(5.0, 4.0));
    RandomGenerator rand = RandomManager.getRandom();
    KMeansUpdateStrategy miniBatch = new MiniBatchUpdateStrategy(100, 2, rand);
    miniBatch.update(points, centers);
  }

  @Test
  public void testKmeansCompute() throws Exception {
    RandomGenerator rg = RandomManager.getRandom();
    KMeans kmeans = new KMeans(KMeansInitStrategy.PLUS_PLUS, new LloydsUpdateStrategy(100));
    Centers centers = kmeans.compute(points, 2, rg);
    assertEquals(new Centers(Vectors.of(1.5, 1.0), Vectors.of(4.5, 3.5)), centers);

    // test cluster collapse
    kmeans = new KMeans(KMeansInitStrategy.RANDOM, new LloydsUpdateStrategy(100));
    centers = kmeans.compute(degeneratePoints, 3, rg);
    assertEquals(new Centers(Vectors.of(1.0, 1.0)), centers);
  }
}
