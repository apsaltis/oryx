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

import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.RealVector;

/**
 * Encapsulates a {@link DecompositionSolver} from Commons Math.
 * 
 * @author Sean Owen
 */
final class CommonsMathSolver implements Solver {
  
  private final DecompositionSolver solver;
  
  CommonsMathSolver(DecompositionSolver solver) {
    this.solver = solver;
  }

  @Override
  public float[] solveDToF(double[] b) {
    RealVector vec = solver.solve(Vectors.of(b));
    float[] result = new float[b.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = (float) vec.getEntry(i);
    }
    return result;
  }

  @Override
  public double[] solveFToD(float[] b) {
    RealVector bVec = Vectors.dense(b.length);
    for (int i = 0; i < b.length; i++) {
      bVec.setEntry(i, b[i]);
    }
    RealVector vec = solver.solve(bVec);
    double[] result = new double[b.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = vec.getEntry(i);
    }
    return result;
  } 

}
