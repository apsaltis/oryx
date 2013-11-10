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

public final class Index {

  private int row;
  private int column;

  public Index() {}

  public Index(int row, int column) {
    this.row = row;
    this.column = column;
  }

  public int getRow() {
    return row;
  }

  public int getColumn() {
    return column;
  }

  public boolean isDiagonal() {
    return row == column;
  }

  @Override
  public int hashCode() {
    return 17 * row + 37 * column;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Index)) {
      return false;
    }
    Index idx = (Index) other;
    return row == idx.row && column == idx.column;
  }

  @Override
  public String toString() {
    return "(" + row + ',' + column + ')';
  }
}
