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

package com.cloudera.oryx.als.common.factorizer.als;


import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.math.MatrixUtils;
import com.cloudera.oryx.als.common.factorizer.MatrixFactorizer;
import com.cloudera.oryx.common.math.MatrixUtilsTest;

/**
 * Tests {@link AlternatingLeastSquares} with negative input values.
 *
 * @author Sean Owen
 */
public final class NegativeInputTest extends OryxTest {

  @Test
  public void testALS() throws Exception {

    LongObjectMap<LongFloatMap> byRow = new LongObjectMap<LongFloatMap>();
    LongObjectMap<LongFloatMap> byCol = new LongObjectMap<LongFloatMap>();

    // Octave: R = [ 1 1 1 0 ; 0 -1 1 1 ; -1 0 0 1 ]
    MatrixUtils.addTo(0, 0,  1.0f, byRow, byCol);
    MatrixUtils.addTo(0, 1,  1.0f, byRow, byCol);
    MatrixUtils.addTo(0, 2,  1.0f, byRow, byCol);
    MatrixUtils.addTo(1, 1, -1.0f, byRow, byCol);
    MatrixUtils.addTo(1, 2,  1.0f, byRow, byCol);
    MatrixUtils.addTo(1, 3,  1.0f, byRow, byCol);
    MatrixUtils.addTo(2, 0, -1.0f, byRow, byCol);
    MatrixUtils.addTo(2, 3,  1.0f, byRow, byCol);

    // Octave: Y = [ 0.1 0.2 ; 0.2 0.5 ; 0.3 0.1 ; 0.2 0.2 ];
    LongObjectMap<float[]> previousY = new LongObjectMap<float[]>();
    previousY.put(0L, new float[] {0.1f, 0.2f});
    previousY.put(1L, new float[] {0.2f, 0.5f});
    previousY.put(2L, new float[] {0.3f, 0.1f});
    previousY.put(3L, new float[] {0.2f, 0.2f});

    MatrixFactorizer als = new AlternatingLeastSquares(byRow, byCol, 2, 0.0001, 40);
    als.setPreviousY(previousY);
    als.call();

    RealMatrix product = MatrixUtilsTest.multiplyXYT(als.getX(), als.getY());

    assertArrayEquals(
        new float[] {0.899032f, 0.900162f, 0.990150f, -0.026642f},
        product.getRow(0));
    assertArrayEquals(
        new float[] {0.181214f, 0.089988f, 0.787198f, 1.012226f},
        product.getRow(1));
    assertArrayEquals(
        new float[] {-0.104165f, -0.178240f, 0.360391f, 0.825856f},
        product.getRow(2));
  }

}
