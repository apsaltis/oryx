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
import java.util.Map;

import org.apache.commons.math3.linear.RealVector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public final class LloydsUpdateStrategy implements KMeansUpdateStrategy {

  private final int numIterations;
  
  public LloydsUpdateStrategy(int numIterations) {
    this.numIterations = numIterations;
  }
  
  @Override
  public <W extends Weighted<RealVector>> Centers update(List<W> points, Centers centers) {
    for (int iter = 0; iter < numIterations; iter++) {
      Map<Integer, List<W>> assignments = Maps.newHashMap();
      for (int i = 0; i < centers.size(); i++) {
        assignments.put(i, Lists.<W>newArrayList());
      }
      for (W weightedVec : points) {
        assignments.get(centers.getDistance(weightedVec.thing()).getClosestCenterId()).add(weightedVec);
      }
      List<RealVector> centroids = Lists.newArrayList();
      for (Map.Entry<Integer, List<W>> e : assignments.entrySet()) {
        if (e.getValue().isEmpty()) {
          centroids.add(centers.get(e.getKey())); // fix the no-op center
        } else {
          centroids.add(centroid(e.getValue()));
        }
      }
      centers = new Centers(centroids);
    }
    return centers;
  }

  /**
   * Compute the {@code Vector} that is the centroid of the given weighted points.
   * 
   * @param points The weighted points
   * @return The centroid of the weighted points
   */
  public static <W extends Weighted<RealVector>> RealVector centroid(Iterable<W> points) {
    RealVector center = null;
    double sz = 0.0;
    for (W v : points) {
      RealVector weighted = v.thing().mapMultiply(v.weight());
      if (center == null) {
        center = weighted;
      } else {
        center = center.add(weighted);
      }
      sz += v.weight();
    }
    return center.mapDivide(sz);
  }  
}
