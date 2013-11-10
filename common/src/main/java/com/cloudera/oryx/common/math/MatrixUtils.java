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

package com.cloudera.oryx.common.math;

import java.lang.reflect.Field;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.ClassUtils;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;

/**
 * Contains utility methods for dealing with matrices, which are here represented as
 * {@link com.cloudera.oryx.common.collection.LongObjectMap}s of {@link com.cloudera.oryx.common.collection.LongFloatMap}s, or of {@code float[]}.
 *
 * @author Sean Owen
 */
public final class MatrixUtils {

  // This hack saves a lot of time spent copying out data from Array2DRowRealMatrix objects
  private static final Field MATRIX_DATA_FIELD = ClassUtils.loadField(Array2DRowRealMatrix.class, "data");
  private static final LinearSystemSolver MATRIX_INVERTER = new CommonsMathLinearSystemSolver();

  private MatrixUtils() {
  }

  /**
   * Efficiently increments an entry in two parallel, sparse matrices.
   *
   * @param row row to increment
   * @param column column to increment
   * @param value increment value
   * @param RbyRow matrix R to update, keyed by row
   * @param RbyColumn matrix R to update, keyed by column
   */
  public static void addTo(long row,
                           long column,
                           float value,
                           LongObjectMap<LongFloatMap> RbyRow,
                           LongObjectMap<LongFloatMap> RbyColumn) {
    addToByRow(row, column, value, RbyRow);
    addToByRow(column, row, value, RbyColumn);
  }

  /**
   * Efficiently increments an entry in a row-major sparse matrix.
   *
   * @param row row to increment
   * @param column column to increment
   * @param value increment value
   * @param RbyRow matrix R to update, keyed by row
   */
  private static void addToByRow(long row,
                                 long column,
                                 float value,
                                 LongObjectMap<LongFloatMap> RbyRow) {

    LongFloatMap theRow = RbyRow.get(row);
    if (theRow == null) {
      theRow = new LongFloatMap();
      RbyRow.put(row, theRow);
    }
    theRow.increment(column, value);
  }

  /**
   * Efficiently removes an entry in two parallel, sparse matrices.
   *
   * @param row row to remove
   * @param column column to remove
   * @param RbyRow matrix R to update, keyed by row
   * @param RbyColumn matrix R to update, keyed by column
   */
  public static void remove(long row,
                            long column,
                            LongObjectMap<LongFloatMap> RbyRow,
                            LongObjectMap<LongFloatMap> RbyColumn) {
    removeByRow(row, column, RbyRow);
    removeByRow(column, row, RbyColumn);
  }
  
  /**
   * Efficiently removes an entry from a row-major sparse matrix.
   *
   * @param row row to remove
   * @param column column to remove
   * @param RbyRow matrix R to update, keyed by row
   */
  private static void removeByRow(long row, long column, LongObjectMap<LongFloatMap> RbyRow) {
    LongFloatMap theRow = RbyRow.get(row);
    if (theRow != null) {
      theRow.remove(column);
      if (theRow.isEmpty()) {
        RbyRow.remove(row);
      }
    }
  }

  /**
   * @return {@link LinearSystemSolver#isNonSingular(RealMatrix)}
   */
  public static boolean isNonSingular(RealMatrix M) {
    return MATRIX_INVERTER.isNonSingular(M);    
  }

  /**
   * @return {@link LinearSystemSolver#getSolver(RealMatrix)}
   */
  public static Solver getSolver(RealMatrix M) {
    return MATRIX_INVERTER.getSolver(M);
  }

  /**
   * @param matrix an {@link Array2DRowRealMatrix}
   * @return its "data" field -- not a copy
   */
  public static double[][] accessMatrixDataDirectly(RealMatrix matrix) {
    try {
      return (double[][]) MATRIX_DATA_FIELD.get(matrix);
    } catch (IllegalAccessException iae) {
      throw new IllegalStateException(iae);
    }
  }

  /**
   * @param M tall, skinny matrix
   * @return MT * M as a dense matrix
   */
  public static RealMatrix transposeTimesSelf(LongObjectMap<float[]> M) {
    if (M == null || M.isEmpty()) {
      return null;
    }
    RealMatrix result = null;
    for (LongObjectMap.MapEntry<float[]> entry : M.entrySet()) {
      float[] vector = entry.getValue();
      int dimension = vector.length;
      if (result == null) {
        result = new Array2DRowRealMatrix(dimension, dimension);
      }
      for (int row = 0; row < dimension; row++) {
        float rowValue = vector[row];
        for (int col = 0; col < dimension; col++) {
          result.addToEntry(row, col, rowValue * vector[col]);
        }
      }
    }
    Preconditions.checkNotNull(result);
    return result;
  }

}
