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
package com.cloudera.oryx.kmeans.computation.covariance;

import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.cloudera.oryx.kmeans.common.Distance;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class AssignFn<V extends RealVector> extends OryxDoFn<V, Pair<ClusterKey, Pair<V, Double>>> {
  private final KSketchIndex centers;
  private final boolean approx;

  public AssignFn(KSketchIndex centers, boolean approx) {
    this.centers = centers;
    this.approx = approx;
  }

  @Override
  public void process(V vec, Emitter<Pair<ClusterKey, Pair<V, Double>>> emitter) {
    Distance[] distances = centers.getDistances(vec, approx);
    int[] pointCounts = centers.getPointCounts();
    for (int i = 0; i < distances.length; i++) {
      ClusterKey key = new ClusterKey(pointCounts[i], distances[i].getClosestCenterId());
      emitter.emit(Pair.of(key, Pair.of(vec, distances[i].getSquaredDistance())));
    }
  }
}
