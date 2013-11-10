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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link CommonsMathLinearSystemSolver}.
 *
 * @author Sean Owen
 */
public final class CommonsMathLinearSystemSolverTest extends OryxTest {

  @Test
  public void testSolveFToD() {
    double[][] data = { new double[] {1.0,2.0}, new double[] {3.0,5.0}};
    RealMatrix M = new Array2DRowRealMatrix(data, false);
    LinearSystemSolver cmLinearSystemSolver = new CommonsMathLinearSystemSolver();
    Solver solver = cmLinearSystemSolver.getSolver(M);
    assertArrayEquals(new double[] {-12.0,7.0}, solver.solveFToD(new float[] {2.0f, -1.0f}));
  }

  @Test
  public void testSolveDToF() {
    double[][] data = { new double[] {1.0,2.0}, new double[] {3.0,5.0}};
    RealMatrix M = new Array2DRowRealMatrix(data, false);
    LinearSystemSolver cmLinearSystemSolver = new CommonsMathLinearSystemSolver();
    Solver solver = cmLinearSystemSolver.getSolver(M);
    assertArrayEquals(new float[] {-12.0f,7.0f}, solver.solveDToF(new double[] {2.0, -1.0}));
  }


}
