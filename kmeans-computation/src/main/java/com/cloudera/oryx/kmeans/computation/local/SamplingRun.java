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

import com.cloudera.oryx.kmeans.common.Distance;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;

import com.google.common.collect.Maps;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.RandomGenerator;

import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;

public final class SamplingRun implements Callable<Collection<RealVector>> {

  private final KSketchIndex index;
  private final RandomGenerator random;
  private final int foldId;
  private final List<RealVector> vecs;
  private final int sampleCount;
  private List<RealVector> sample;

  public SamplingRun(KSketchIndex index, RandomGenerator random, int foldId, List<RealVector> vecs, int sampleCount) {
    this.index = index;
    this.random = random;
    this.foldId = foldId;
    this.vecs = vecs;
    this.sampleCount = sampleCount;
  }

  @Override
  public Collection<RealVector> call() throws Exception {
    SortedMap<Double, RealVector> reservoir = Maps.newTreeMap();
    for (RealVector v : vecs) {
      Distance d = index.getDistance(v, foldId, true);
      if (d.getSquaredDistance() > 0.0) {
        double score = Math.log(random.nextDouble()) / d.getSquaredDistance();
        if (reservoir.size() < sampleCount) {
          reservoir.put(score, v);
        } else if (score > reservoir.firstKey()) {
          reservoir.remove(reservoir.firstKey());
          reservoir.put(score, v);
        }
      }
    }
    return reservoir.values();
  }
}
