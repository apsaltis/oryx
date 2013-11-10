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

import java.util.concurrent.ExecutionException;

import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.als.common.factorizer.MatrixFactorizer;
import com.cloudera.oryx.common.math.MatrixUtils;
import com.cloudera.oryx.common.math.MatrixUtilsTest;

/**
 * Tests {@link AlternatingLeastSquares}.
 *
 * @author Sean Owen
 */
public final class AlternatingLeastSquaresTest extends OryxTest {

  @Test
  public void testALS() throws Exception {
    RealMatrix product = buildTestXYTProduct();

    assertArrayEquals(
        new float[] {-0.030258f, 0.852781f, 1.004839f, 1.024087f, -0.036206f},
        product.getRow(0));
    assertArrayEquals(
        new float[] {0.077046f, 0.751232f, 0.949796f, 0.910322f, 0.073047f},
        product.getRow(1));
    assertArrayEquals(
        new float[] {0.916777f, -0.196005f, 0.335926f, -0.163591f, 0.929028f},
        product.getRow(2));
    assertArrayEquals(
        new float[] {0.987400f, 0.130943f, 0.772403f, 0.235522f, 0.998354f},
        product.getRow(3));
    assertArrayEquals(
        new float[] {-0.028683f, 0.850540f, 1.003130f, 1.021514f, -0.034598f},
        product.getRow(4));
  }

  static RealMatrix buildTestXYTProduct() throws ExecutionException, InterruptedException {

    LongObjectMap<LongFloatMap> byRow = new LongObjectMap<LongFloatMap>();
    LongObjectMap<LongFloatMap> byCol = new LongObjectMap<LongFloatMap>();
    // Octave: R = [ 0 2 3 1 0 ; 0 0 4 5 0 ; 1 0 0 0 2 ; 3 0 1 0 5 ; 0 2 2 2 0 ]
    MatrixUtils.addTo(0, 1, 2.0f, byRow, byCol);
    MatrixUtils.addTo(0, 2,  3.0f, byRow, byCol);
    MatrixUtils.addTo(0, 3,  1.0f, byRow, byCol);
    MatrixUtils.addTo(1, 2,  4.0f, byRow, byCol);
    MatrixUtils.addTo(1, 3,  5.0f, byRow, byCol);
    MatrixUtils.addTo(2, 0,  1.0f, byRow, byCol);
    MatrixUtils.addTo(2, 4,  2.0f, byRow, byCol);
    MatrixUtils.addTo(3, 0,  3.0f, byRow, byCol);
    MatrixUtils.addTo(3, 2,  1.0f, byRow, byCol);
    MatrixUtils.addTo(3, 4,  5.0f, byRow, byCol);
    MatrixUtils.addTo(4, 1,  2.0f, byRow, byCol);
    MatrixUtils.addTo(4, 2,  2.0f, byRow, byCol);
    MatrixUtils.addTo(4, 3,  2.0f, byRow, byCol);

    // Octave: Y = [ 0.1 0.2 ; 0.2 0.5 ; 0.3 0.1 ; 0.2 0.2 ; 0.5 0.4 ];
    LongObjectMap<float[]> previousY = new LongObjectMap<float[]>();
    previousY.put(0L, new float[] {0.1f, 0.2f});
    previousY.put(1L, new float[] {0.2f, 0.5f});
    previousY.put(2L, new float[] {0.3f, 0.1f});
    previousY.put(3L, new float[] {0.2f, 0.2f});
    previousY.put(4L, new float[] {0.5f, 0.4f});

    MatrixFactorizer als = new AlternatingLeastSquares(byRow, byCol, 2, 0.0001, 40);
    als.setPreviousY(previousY);
    als.call();

    return MatrixUtilsTest.multiplyXYT(als.getX(), als.getY());
  }

}
