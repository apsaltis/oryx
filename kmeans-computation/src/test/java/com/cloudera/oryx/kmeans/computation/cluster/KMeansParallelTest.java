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

package com.cloudera.oryx.kmeans.computation.cluster;

import java.util.List;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.kmeans.common.Centers;

import com.cloudera.oryx.kmeans.computation.MLAvros;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.oryx.common.math.Vectors;
import com.google.common.collect.ImmutableList;

public final class KMeansParallelTest extends OryxTest {
  
  private static final PCollection<RealVector> VECS = MemPipeline.typedCollectionOf(
      MLAvros.vector(),
      Vectors.of(2.0, 1.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(5.0, 4.0),
      Vectors.of(4.0, 3.0),
      Vectors.of(2.0, 1.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(5.0, 4.0),
      Vectors.of(4.0, 3.0),
      Vectors.of(2.0, 1.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(5.0, 4.0),
      Vectors.of(4.0, 3.0),
      Vectors.of(2.0, 1.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(5.0, 4.0),
      Vectors.of(4.0, 3.0));
  
  private KMeansParallel kmp;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    kmp = new KMeansParallel(RandomManager.getRandom(), 128, 32);
  }
  
  @Test
  public void testLloyds() throws Exception {
    List<Centers> centers = ImmutableList.of(
        new Centers(ImmutableList.of(Vectors.of(1.0, 1.0), Vectors.of(5.0, 4.0))));
    List<Centers> res = kmp.lloydsAlgorithm(VECS, centers, 0, false);
    assertEquals(centers, res);
    
    res = kmp.lloydsAlgorithm(VECS, res, 1, false);
    List<Centers> expected = ImmutableList.of(
        new Centers(ImmutableList.of(Vectors.of(1.5, 1.0), Vectors.of(4.5, 3.5))));
    assertEquals(expected, res);
    
    res = kmp.lloydsAlgorithm(VECS, res, 1, false);
    assertEquals(expected, res);
  }
}
