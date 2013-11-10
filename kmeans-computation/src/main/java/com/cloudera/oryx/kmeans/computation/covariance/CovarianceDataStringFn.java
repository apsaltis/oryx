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

import com.cloudera.oryx.computation.common.fn.OryxMapFn;
import org.apache.crunch.Pair;

public final class CovarianceDataStringFn extends OryxMapFn<Pair<Pair<ClusterKey, Index>, CoMoment>, String> {
  @Override
  public String map(Pair<Pair<ClusterKey, Index>, CoMoment> input) {
    ClusterKey key = input.first().first();
    Index idx = input.first().second();
    CoMoment cm = input.second();
    return new CovarianceData(
        key.getK(),
        key.getCenterId(),
        idx.getRow(),
        idx.getColumn(),
        cm.getMeanX(),
        cm.getCovariance()).toString();
  }
}
