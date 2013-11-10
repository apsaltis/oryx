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
package com.cloudera.oryx.kmeans.computation.outlier;

import com.cloudera.oryx.common.math.NamedRealVector;
import com.cloudera.oryx.computation.common.fn.OryxMapFn;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.SimpleRecord;
import com.cloudera.oryx.computation.common.records.Spec;
import com.cloudera.oryx.kmeans.computation.covariance.ClusterKey;
import com.cloudera.oryx.kmeans.computation.covariance.DistanceData;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Pair;

import java.io.IOException;
import java.util.Map;

final class OutlierScoreFn extends OryxMapFn<Pair<ClusterKey, Pair<NamedRealVector, Double>>, Record> {
  private final Spec spec;
  private final String covarianceKey;
  private final int n;
  private Map<ClusterKey, DistanceData> distances;

  OutlierScoreFn(Spec spec, String covarianceKey, int n) {
    this.spec = spec;
    this.covarianceKey = covarianceKey;
    this.n = n;
  }

  @Override
  public void initialize() {
    super.initialize();
    if (covarianceKey != null) {
      try {
        distances = DistanceData.load(covarianceKey, n);
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  @Override
  public Record map(Pair<ClusterKey, Pair<NamedRealVector, Double>> input) {
    SimpleRecord r = new SimpleRecord(spec);
    ClusterKey key = input.first();
    NamedRealVector mlvec = input.second().first();
    double centerDistance = input.second().second();
    r.set("vector_id", mlvec.getName());
    r.set("k", key.getK());
    r.set("closest_center_id", key.getCenterId());
    r.set("center_distance", centerDistance);
    if (distances != null) {
      DistanceData distanceData = distances.get(key);
      if (distanceData != null) {
        r.set("euclidean_distance", distanceData.euclideanDistance(mlvec));
        r.set("outlier_distance", distanceData.mahalanobisDistance(mlvec));
      } else {
        //TODO: can this ever happen?
        r.set("euclidean_distance", -1);
        r.set("outlier_distance", -1);
      }
    }
    return r;
  }
}
