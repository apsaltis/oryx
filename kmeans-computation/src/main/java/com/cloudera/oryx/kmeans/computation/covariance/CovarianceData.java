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

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.io.DelimitedDataUtils;

public final class CovarianceData {

  private final int clusteringId;
  private final int centerId;
  private final int row;
  private final int column;
  private final double meanX;
  private final double cov;

  public static CovarianceData parse(String line) {
    String[] pieces = DelimitedDataUtils.decode(line);
    Preconditions.checkArgument(pieces.length == 6, "Invalid covariance data: %s", line);
    return new CovarianceData(
        Integer.valueOf(pieces[0]),
        Integer.valueOf(pieces[1]),
        Integer.valueOf(pieces[2]),
        Integer.valueOf(pieces[3]),
        Double.valueOf(pieces[4]),
        Double.valueOf(pieces[5]));
  }

  public CovarianceData(int clusteringId, int centerId, int row, int column, double meanX, double cov) {
    this.clusteringId = clusteringId;
    this.centerId = centerId;
    this.row = row;
    this.column = column;
    this.meanX = meanX;
    this.cov = cov;
  }

  public int getClusteringId() {
    return clusteringId;
  }

  public int getCenterId() {
    return centerId;
  }

  public int getRow() {
    return row;
  }

  public int getColumn() {
    return column;
  }

  public double getMeanX() {
    return meanX;
  }

  public double getCov() {
    return cov;
  }

  @Override
  public String toString() {
    return DelimitedDataUtils.encode(clusteringId, centerId, row, column, meanX, cov);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CovarianceData)) {
      return false;
    }

    CovarianceData other = (CovarianceData) o;

    return centerId == other.centerId &&
        clusteringId == other.clusteringId &&
        column == other.column &&
        Double.compare(other.cov, cov) == 0 &&
        Double.compare(other.meanX, meanX) == 0 &&
        row == other.row;
  }

  @Override
  public int hashCode() {
    int result = clusteringId;
    result = 31 * result + centerId;
    result = 31 * result + row;
    result = 31 * result + column;
    long temp = Double.doubleToLongBits(meanX);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(cov);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}
