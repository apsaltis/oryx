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

package com.cloudera.oryx.als.serving;

import java.util.List;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.als.common.PairRescorer;

/**
 * Convenience implementation that will aggregate the behavior of multiple {@link PairRescorer}s.
 * It will filter an item if any of the given instances filter it, and will rescore
 * by applying the rescorings in the given order.
 *
 * @author Sean Owen
 * @see MultiRescorer
 * @see MultiRescorerProvider
 */
public final class MultiLongPairRescorer implements PairRescorer {

  private final PairRescorer[] rescorers;

  public MultiLongPairRescorer(List<PairRescorer> rescorers) {
    Preconditions.checkNotNull(rescorers);
    Preconditions.checkState(!rescorers.isEmpty());
    PairRescorer[] newArray = new PairRescorer[rescorers.size()];
    this.rescorers = rescorers.toArray(newArray);
  }

  @Override
  public double rescore(String a, String b, double value) {
    for (PairRescorer rescorer : rescorers) {
      value = rescorer.rescore(a, b, value);
      if (Double.isNaN(value)) {
        return Double.NaN;
      }
    }
    return value;
  }

  @Override
  public boolean isFiltered(String a, String b) {
    for (PairRescorer rescorer : rescorers) {
      if (rescorer.isFiltered(a, b)) {
        return true;
      }
    }
    return false;
  }

}
