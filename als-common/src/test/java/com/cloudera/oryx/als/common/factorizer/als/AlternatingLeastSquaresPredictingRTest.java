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
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Tests {@link AlternatingLeastSquares} when {@code model.reconstruct-r-matrix=true}.
 *
 * @author Sean Owen
 */
public final class AlternatingLeastSquaresPredictingRTest extends OryxTest {

  @Test
  public void testALSPredictingR() throws Exception {

    ConfigUtils.overlayConfigOnDefault(getResourceAsFile("AlternatingLeastSquaresPredictingRTest.conf"));

    RealMatrix product = AlternatingLeastSquaresTest.buildTestXYTProduct();

    assertArrayEquals(
        new float[] {0.0678369f, 0.6574759f, 2.1020291f, 2.0976211f, 0.1115919f},
        product.getRow(0));
    assertArrayEquals(
        new float[] {-0.0176293f, 1.3062225f, 4.1365933f, 4.1739127f, -0.0380586f},
        product.getRow(1));
    assertArrayEquals(
        new float[] {1.0854513f, -0.0344434f, 0.1725342f, -0.1564803f, 1.8502977f},
        product.getRow(2));
    assertArrayEquals(
        new float[] {2.8377915f, 0.0528524f, 0.9041158f, 0.0474437f, 4.8365208f},
        product.getRow(3));
    assertArrayEquals(
        new float[] {-0.0057799f, 0.6608552f, 2.0936351f, 2.1115670f, -0.0139042f,},
        product.getRow(4));
  }

}
