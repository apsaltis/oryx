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
package com.cloudera.oryx.kmeans.common;

import com.google.common.base.Preconditions;

import java.io.Serializable;

public final class Distance implements Serializable {

  private final int centerId;
  private final double squaredDistance;

  public Distance(double squaredDistance, int centerId) {
    Preconditions.checkArgument(centerId >= 0, "Invalid closest center ID: + " + centerId);
    this.squaredDistance = squaredDistance;
    this.centerId = centerId;
  }

  public int getClosestCenterId() {
    return centerId;
  }

  public double getSquaredDistance() {
    return squaredDistance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Distance other = (Distance) o;
    return centerId == other.centerId && Double.compare(other.squaredDistance, squaredDistance) == 0;
  }

  @Override
  public int hashCode() {
    int result = centerId;
    long temp = Double.doubleToLongBits(squaredDistance);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}
