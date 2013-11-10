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
package com.cloudera.oryx.kmeans.computation.local;

import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import org.apache.commons.math3.linear.RealVector;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

public final class AssignmentRun implements Callable<List<WeightedRealVector>> {

  private final KSketchIndex index;
  //private final RandomGenerator random;
  private final int foldId;
  private final List<RealVector> vecs;

  public AssignmentRun(KSketchIndex index, int foldId, List<RealVector> vecs) {
    this.index = index;
    //this.random = random;
    this.foldId = foldId;
    this.vecs = vecs;
  }

  @Override
  public List<WeightedRealVector> call() throws IOException {
    long[] cnts = new long[index.getPointCounts()[foldId]];
    for (RealVector v : vecs) {
      cnts[index.getDistance(v, foldId, true).getClosestCenterId()]++;
    }
    return index.getWeightedVectorsForFold(foldId, cnts);
  }
}
