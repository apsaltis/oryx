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

package com.cloudera.oryx.als.computation.types;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

import java.util.Arrays;

public final class MatrixRow {

  private static final float[] NO_VALUES = new float[0];

  public static final MapFn<MatrixRow, Pair<Long, float[]>> AS_PAIR =
      new MapFn<MatrixRow, Pair<Long, float[]>>() {
        @Override
        public Pair<Long, float[]> map(MatrixRow input) {
          return Pair.of(input.getRowId(), input.getValues());
        }
      };

  private final long rowId;
  private final float[] values;

  public MatrixRow() {
    this(0, NO_VALUES);
  }

  public MatrixRow(long rowId, float[] values) {
    Preconditions.checkNotNull(values);
    this.rowId = rowId;
    this.values = values;
  }

  public long getRowId() {
    return rowId;
  }

  public float[] getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MatrixRow)) {
      return false;
    }
    MatrixRow other = (MatrixRow) o;
    return rowId == other.rowId && Arrays.equals(values, other.values);
  }

  @Override
  public int hashCode() {
    return Longs.hashCode(rowId) ^ Arrays.hashCode(values);
  }

}
