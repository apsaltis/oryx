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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.linear.RealVector;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.math3.random.RandomGenerator;

/**
 * The strategies used to choose the initial {@link Centers} used for a k-means algorithm
 * run prior to running Lloyd's algorithm.
 */
public enum KMeansInitStrategy {

  /**
   * Uses the classic random selection strategy to create the initial {@code Centers}. The
   * algorithm will randomly choose K points from the input points, favoring points with
   * higher weights.
   */
  RANDOM {
    @Override
    public <W extends Weighted<RealVector>> Centers apply(List<W> points,
                                                          int numClusters,
                                                          RandomGenerator random) {
      return new Centers(
          Lists.transform(Weighted.sample(points, numClusters, random), new Function<W, RealVector>() {
            @Override
            public RealVector apply(W wt) {
              return wt.thing();
            }
          }));
    }
  },
  
  /**
   * Uses the <i>k-means++</i> strategy described in Arthur and Vassilvitskii (2007).
   * See <a href="http://en.wikipedia.org/wiki/K-means%2B%2B">the Wikipedia page</a>
   * for details.
   */
  PLUS_PLUS {
    @Override
    public <W extends Weighted<RealVector>> Centers apply(List<W> points,
                                                int numClusters,
                                                RandomGenerator random) {
      Centers centers = RANDOM.apply(points, 1, random);
      double[] cumulativeScores = new double[points.size() + 1];
      for (int i = 1; i < numClusters; i++) {
        cumulativeScores[0] = 0;
        for (int j = 0; j < points.size(); j++) {
          W wv = points.get(j);
          double score = centers.getDistance(wv.thing()).getSquaredDistance() * wv.weight();
          cumulativeScores[j + 1] = cumulativeScores[j] + score;
        }
        double r = cumulativeScores[points.size()] * random.nextDouble();
        int next = Arrays.binarySearch(cumulativeScores, r);
        int index = (next > 0) ? next - 1 : -2 - next;
        while (index > 0 && centers.contains(points.get(index).thing())) {
          index--;
        }
        centers = centers.extendWith(points.get(index).thing());
      }
      return centers;
    }
  };

  /**
   * Use this instance to create the initial {@code Centers} from the given parameters.
   * 
   * @param points The candidate {@code WeightedVec} instances for the cluster
   * @param numClusters The number of points in the center (i.e., the "k" in "k-means")
   * @param randomGenerator The {@code RandomGenerator} instance to use
   * @return A new {@code Centers} instance created using this instance
   */
  public abstract <W extends Weighted<RealVector>> Centers apply(List<W> points, int numClusters,
                                                       RandomGenerator randomGenerator);
}
