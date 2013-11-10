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

package com.cloudera.oryx.kmeans.computation.covariance;

import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.servcomp.Store;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public final class DistanceData implements Serializable {

  private final RealVector means;
  private final RealMatrix covInv;

  public static Map<ClusterKey, DistanceData> load(String prefix, int n) throws IOException {
    Map<ClusterKey, CovarianceDataBuilder> db = Maps.newHashMap();

    Store store = Store.get();
    for (String file : store.list(prefix, true)) {
      for (String line : new FileLineIterable(store.readFrom(file))) {
        CovarianceData cd = CovarianceData.parse(line);
        ClusterKey key = new ClusterKey(cd.getClusteringId(), cd.getCenterId());
        CovarianceDataBuilder cdb = db.get(key);
        if (cdb == null) {
          cdb = new CovarianceDataBuilder(n);
          db.put(key, cdb);
        }
        cdb.update(cd);
      }
    }

    return Maps.transformValues(db, new Function<CovarianceDataBuilder, DistanceData>() {
      @Override
      public DistanceData apply(CovarianceDataBuilder input) {
        return input.getDistanceData();
      }
    });
  }

  public DistanceData(RealVector means) {
    this(means, null);
  }

  public DistanceData(RealVector means, RealMatrix covInv) {
    this.means = means;
    this.covInv = covInv;
  }

  public boolean hasCovariance() {
    return covInv != null;
  }

  public double euclideanDistance(RealVector v) {
    return means.getDistance(v);
  }

  public double mahalanobisDistance(RealVector v) {
    RealVector d = v.subtract(means);
    return d.dotProduct(covInv.operate(d));
  }
}
