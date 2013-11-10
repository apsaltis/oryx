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

import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.google.common.collect.Lists;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.util.ArrayList;
import java.util.List;

public final class KMeansClusteringFn extends OryxDoFn<
    Pair<Pair<Integer, Integer>, Iterable<Pair<Integer, WeightedRealVector>>>,
    KMeansEvaluationData> {

  private final EvaluationSettings settings;
  private final int folds;

  public KMeansClusteringFn(EvaluationSettings settings) {
    this.settings = settings;
    this.folds = settings.getFolds();
  }

  @Override
  public void process(Pair<Pair<Integer, Integer>, Iterable<Pair<Integer, WeightedRealVector>>> input,
                      Emitter<KMeansEvaluationData> emitter) {
    int k = input.first().first();
    int replica = input.first().second();
    List<List<WeightedRealVector>> foldSketches = Lists.newArrayList();
    for (int fold = 0; fold < folds; fold++) {
      foldSketches.add(new ArrayList<WeightedRealVector>());
    }
    for (Pair<Integer, WeightedRealVector> fwv : input.second()) {
      int fold = fwv.first();
      foldSketches.get(fold).add(fwv.second());
    }
    KMeansEvaluationData data = new KMeansEvaluationData(foldSketches, k, replica, settings);
    emitter.emit(data);
  }
}
