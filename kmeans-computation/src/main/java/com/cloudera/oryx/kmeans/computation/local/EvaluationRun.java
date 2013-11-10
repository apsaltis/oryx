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
import com.cloudera.oryx.kmeans.computation.evaluate.EvaluationSettings;
import com.cloudera.oryx.kmeans.computation.evaluate.KMeansEvaluationData;

import java.util.List;
import java.util.concurrent.Callable;

final class EvaluationRun implements Callable<KMeansEvaluationData> {

  private final List<List<WeightedRealVector>> sketches;
  private final int k;
  private final int replica;
  private final EvaluationSettings settings;

  EvaluationRun(List<List<WeightedRealVector>> sketches, int k, int replica, EvaluationSettings settings) {
    this.sketches = sketches;
    this.k = k;
    this.replica = replica;
    this.settings = settings;
  }

  @Override
  public KMeansEvaluationData call() throws Exception {
    return new KMeansEvaluationData(sketches, k, replica, settings);
  }
}
