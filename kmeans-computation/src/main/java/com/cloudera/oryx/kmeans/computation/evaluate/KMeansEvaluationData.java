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

package com.cloudera.oryx.kmeans.computation.evaluate;

import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.kmeans.common.Centers;
import com.cloudera.oryx.kmeans.common.ClusterValidityStatistics;
import com.cloudera.oryx.kmeans.common.KMeans;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

public final class KMeansEvaluationData implements Serializable {

  private final List<List<WeightedRealVector>> sketchPoints;
  private final EvaluationSettings settings;
  private final int k;
  private final int replica;

  private Centers best;
  private ClusterValidityStatistics clusterValidityStatistics;

  public KMeansEvaluationData(List<List<WeightedRealVector>> sketchPoints,
                              int k, int replica,
                              EvaluationSettings settings) {
    this.sketchPoints = sketchPoints;
    this.k = k;
    this.replica = replica;
    this.settings = settings;
    compute();
  }

  private void compute() {
    KMeans kmeans = new KMeans(settings.getInitStrategy(), settings.getUpdateStrategy());
    List<Centers> centers = Lists.newArrayList();
    for (List<WeightedRealVector> sketchPoint : sketchPoints) {
      centers.add(kmeans.compute(sketchPoint, k, RandomManager.getSeededRandom(replica + 31L * k)));
    }

    ClusterValidityStatistics first = ClusterValidityStatistics.create(
        sketchPoints.get(0),
        centers.get(0),
        centers.get(1),
        replica);

    ClusterValidityStatistics second = ClusterValidityStatistics.create(
        sketchPoints.get(1),
        centers.get(1),
        centers.get(0),
        replica);

    if (first.getTotalCost() < second.getTotalCost()) {
      clusterValidityStatistics = first;
      best = centers.get(0);
    } else {
      clusterValidityStatistics = second;
      best = centers.get(1);
    }
  }

  public int getK() {
    return k;
  }

  public int getReplica() {
    return replica;
  }

  public Centers getBest() {
    return best;
  }

  public ClusterValidityStatistics getClusterValidityStatistics() {
    return clusterValidityStatistics;
  }

  public String getName(String prefix) {
    return prefix + ':' + k + ':' + replica;
  }
}
