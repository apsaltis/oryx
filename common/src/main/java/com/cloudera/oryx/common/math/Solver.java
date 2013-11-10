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

/**
 * A solver for the system Ax = b, where A is an n x n matrix and x and b are n-element vectors.
 * An implementation of this class encapsulates a solver which implicitly contains A.
 * 
 * @author Sean Owen
 */
public interface Solver {
  
  /**
   * Solves a linear system Ax = b, where {@code A} is implicit in this instance.
   * 
   * @param b vector, as {@code double} array
   * @return x as {@code float} array
   */
  float[] solveDToF(double[] b);
  
  /**
   * Like {@link #solveDToF(double[])} but input is a {@code float} array and output is a 
   * {@code double} array.
   */
  double[] solveFToD(float[] b);
  
}
