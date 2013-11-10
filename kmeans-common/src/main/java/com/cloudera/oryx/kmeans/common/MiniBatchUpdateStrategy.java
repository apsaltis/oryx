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

import com.cloudera.oryx.common.random.RandomManager;

import com.google.common.collect.Lists;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.RandomGenerator;

public final class MiniBatchUpdateStrategy implements KMeansUpdateStrategy {

  private final int numIterations;
  private final int miniBatchSize;
  private final RandomGenerator random;
  
  public MiniBatchUpdateStrategy(int numIterations, int miniBatchSize, RandomGenerator random) {
    this.numIterations = numIterations;
    this.miniBatchSize = miniBatchSize;
    this.random = (random == null) ? RandomManager.getRandom() : random;
  }
  
  @Override
  public <W extends Weighted<RealVector>> Centers update(List<W> points, Centers centers) {
    int[] perCenterStepCounts = new int[centers.size()];
    WeightedSampler<RealVector, W> sampler = new WeightedSampler<RealVector, W>(points, random);
    for (int iter = 0; iter < numIterations; iter++) {
      // Compute closest cent for each mini-batch
      List<List<RealVector>> centerAssignments = Lists.newArrayList();
      for (int i = 0; i < centers.size(); i++) {
        centerAssignments.add(Lists.<RealVector>newArrayList());
      }
      for (int i = 0; i < miniBatchSize; i++) {
        RealVector sample = sampler.sample();
        int closestId = centers.getDistance(sample).getClosestCenterId();
        centerAssignments.get(closestId).add(sample);
      }
      // Apply the mini-batch
      List<RealVector> nextCenters = Lists.newArrayList();
      for (int i = 0; i < centerAssignments.size(); i++) {
        RealVector currentCenter = centers.get(i);
        for (int j = 0; j < centerAssignments.get(i).size(); j++) {
          double eta = 1.0 / (++perCenterStepCounts[i] + 1.0);
          currentCenter = currentCenter.mapMultiply(1.0 - eta);
          currentCenter = currentCenter.add(centerAssignments.get(i).get(j).mapMultiply(eta));
        }
        nextCenters.add(currentCenter);
      }
      centers = new Centers(nextCenters);
    }
    return centers;
  }
}
