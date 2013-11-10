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

import com.cloudera.oryx.common.OryxTest;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;

/**
 * Tests {@link MatrixUtils}.
 * 
 * @author Sean Owen
 */
public final class MatrixUtilsTest extends OryxTest {

  public static RealMatrix multiplyXYT(LongObjectMap<float[]> X, LongObjectMap<float[]> Y) {
    int Ysize = Y.size();
    int Xsize = X.size();
    RealMatrix result = new Array2DRowRealMatrix(Xsize, Ysize);
    for (int row = 0; row < Xsize; row++) {
      for (int col = 0; col < Ysize; col++) {
        result.setEntry(row, col, SimpleVectorMath.dot(X.get(row), Y.get(col)));
      }
    }
    return result;
  }

  @Test
  public void testAddTo() {
    LongObjectMap<LongFloatMap> byRow = new LongObjectMap<LongFloatMap>();
    assertNull(byRow.get(0L));
    assertNull(byRow.get(1L));
    assertNull(byRow.get(4L));
    LongObjectMap<LongFloatMap> byCol = new LongObjectMap<LongFloatMap>();
    MatrixUtils.addTo(0L, 0L, -1.0f, byRow, byCol);
    MatrixUtils.addTo(4L, 1L, 2.0f, byRow, byCol);
    assertEquals(-1.0f, byRow.get(0L).get(0L));
    assertEquals(-1.0f, byCol.get(0L).get(0L));
    assertNull(byRow.get(1L));
    assertEquals(2.0f, byRow.get(4L).get(1L));
    assertEquals(2.0f, byCol.get(1L).get(4L));
    assertNaN(byRow.get(4L).get(0L));
  }

  @Test
  public void testRemove() {
    LongObjectMap<LongFloatMap> byRow = new LongObjectMap<LongFloatMap>();
    LongObjectMap<LongFloatMap> byCol = new LongObjectMap<LongFloatMap>();
    MatrixUtils.addTo(0L, 0L, -1.0f, byRow, byCol);
    MatrixUtils.addTo(4L, 1L, 2.0f, byRow, byCol);
    MatrixUtils.remove(0L, 0L, byRow, byCol);
    assertNull(byRow.get(0L));
    assertEquals(2.0f, byRow.get(4L).get(1L));
    assertEquals(2.0f, byCol.get(1L).get(4L));
  }

  @Test
  public void testTransposeTimesSelf() {
    LongObjectMap<float[]> M = new LongObjectMap<float[]>();
    M.put(1L, new float[] {4.0f, -1.0f, -5.0f});
    M.put(2L, new float[] {2.0f, 0.0f, 3.0f});
    RealMatrix MTM = MatrixUtils.transposeTimesSelf(M);
    assertArrayEquals(new double[]{20.0, -4.0, -14.0}, MTM.getRow(0));
    assertArrayEquals(new double[]{-4.0, 1.0, 5.0}, MTM.getRow(1));
    assertArrayEquals(new double[]{-14.0, 5.0, 34.0}, MTM.getRow(2));
  }

  @Test
  public void testMultiplyXYT() {
    LongObjectMap<float[]> X = new LongObjectMap<float[]>();
    X.put(0L, new float[] {2.0f, 3.5f});
    X.put(1L, new float[] {-1.0f, 0.0f});
    LongObjectMap<float[]> Y = new LongObjectMap<float[]>();
    Y.put(0L, new float[] {0.0f, -3.0f});
    Y.put(1L, new float[] {-0.5f, 1.5f});
    RealMatrix product = multiplyXYT(X, Y);
    assertEquals(-10.5, product.getEntry(0, 0));
    assertEquals(4.25, product.getEntry(0, 1));
    assertEquals(0.0, product.getEntry(1, 0));
    assertEquals(0.5, product.getEntry(1, 1));
  }

  @Test
  public void testAccess() {
    double[][] data = { new double[] {1}, };
    RealMatrix matrix = new Array2DRowRealMatrix(data, false);
    assertSame(data, MatrixUtils.accessMatrixDataDirectly(matrix));
  }

}
