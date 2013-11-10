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

import com.google.common.collect.ImmutableList;

import java.util.List;

public final class FixedKEvalStrategy implements KMeansEvalStrategy {

  private final int k;

  public FixedKEvalStrategy(int k) {
    this.k = k;
  }

  @Override
  public List<ClusterValidityStatistics> evaluate(List<ClusterValidityStatistics> stats) {
    ClusterValidityStatistics best = null;
    for (ClusterValidityStatistics cvs : stats) {
      if (cvs.getK() == k) {
        if (best == null || best.getTotalCost() > cvs.getTotalCost()) {
          best = cvs;
        }
      }
    }
    return best == null ? ImmutableList.<ClusterValidityStatistics>of() : ImmutableList.of(best);
  }
}
