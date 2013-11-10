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

import java.io.Serializable;

public final class ClusterKey implements Serializable {

  private int k;
  private int centerId;

  public ClusterKey() { }

  public ClusterKey(int k, int centerId) {
    this.k = k;
    this.centerId = centerId;
  }

  public int getK() {
    return k;
  }

  public int getCenterId() {
    return centerId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ClusterKey)) {
      return false;
    }

    ClusterKey other = (ClusterKey) o;
    return centerId == other.centerId && k == other.k;
  }

  @Override
  public int hashCode() {
    int result = k;
    result = 31 * result + centerId;
    return result;
  }
}
