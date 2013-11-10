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

package com.cloudera.oryx.kmeans.computation.evaluate;

import java.io.Serializable;
import java.util.Arrays;

public final class ClosestSketchVectorData implements Serializable {

  private static final long[] NO_COUNTS = new long[0];

  private int numFolds;
  //private int numCenters;
  private final long[] foldCenterCounts;

  public ClosestSketchVectorData() {
    this.foldCenterCounts = NO_COUNTS;
  }

  public ClosestSketchVectorData(int numFolds, int numCenters) {
    this.numFolds = numFolds;
    //this.numCenters = numCenters;
    this.foldCenterCounts = new long[numFolds * numCenters];
  }

  public int getNumFolds() {
    return numFolds;
  }

  //public int getCenters() {
  //  return numCenters;
  //}

  public long get(int foldId, int centerId) {
    return foldCenterCounts[foldId + numFolds * centerId];
  }

  public void inc(int foldId, int centerId) {
    inc(foldId, centerId, 1L);
  }

  public void inc(int foldId, int centerId, long count) {
    this.foldCenterCounts[foldId + numFolds * centerId] += count;
  }

  public void update(ClosestSketchVectorData other) {
    if (foldCenterCounts.length != other.foldCenterCounts.length) {
      throw new IllegalArgumentException(String.format("Length mismatch: Ours is %d, theirs is %d",
          foldCenterCounts.length, other.foldCenterCounts.length));
    }
    for (int i = 0; i < foldCenterCounts.length; i++) {
      foldCenterCounts[i] += other.foldCenterCounts[i];
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ClosestSketchVectorData ccd = (ClosestSketchVectorData) o;
    return Arrays.equals(foldCenterCounts, ccd.foldCenterCounts);
  }

  @Override
  public int hashCode() {
    return 17 + Arrays.hashCode(foldCenterCounts);
  }
}
