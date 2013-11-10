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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularValueDecomposition;

final class CovarianceDataBuilder {
  private final RealVector means;
  private final RealMatrix rawCoMoment;

  CovarianceDataBuilder(int n) {
    this.means = new ArrayRealVector(n);
    this.rawCoMoment = new Array2DRowRealMatrix(n, n);
  }

  public void update(CovarianceData cm) {
    double cov = cm.getCov();
    rawCoMoment.setEntry(cm.getRow(), cm.getColumn(), cov);

    if (cm.getRow() == cm.getColumn()) {
      means.setEntry(cm.getRow(), cm.getMeanX());
    }
  }

  public DistanceData getDistanceData() {
    return new DistanceData(means,
        new SingularValueDecomposition(rawCoMoment).getSolver().getInverse());
  }
}
