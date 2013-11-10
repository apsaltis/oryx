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

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

/**
 * Factory methods for working with {@code Vector} objects.
 */
public final class Vectors {

  /**
   * Creates a dense {@link RealVector} instance from the given values.
   * The data is <em>not copied</em> for performance.
   * 
   * @param values The array of values to turn into a {@link RealVector}
   */
  public static RealVector of(double... values) {
    return new ArrayRealVector(values, false);
  }

  /**
   * Constructs a dense {@link RealVector} of the given dimension.
   *
   * @param dimension The size of the dense vector to create
   */
  public static RealVector dense(int dimension) {
    return new ArrayRealVector(dimension);
  }

  /**
   * Constructs a spasre {@link RealVector} of the given dimension.
   *
   * @param dimension The dimension of the sparse vector to create
   */
  public static SparseRealVector sparse(int dimension) {
    return new OpenMapRealVector(dimension);
  }

  /**
   * Constructs a spasre {@link RealVector} of the given dimension.
   *
   * @param dimension The dimension of the sparse vector to create
   */
  public static SparseRealVector sparse(int dimension, int expectedSize) {
    return new OpenMapRealVector(dimension, expectedSize);
  }

  /**
   * @return a vector of the same sparse-ness and dimension as {@code vec} but without data
   */
  public static RealVector like(RealVector vector) {
    int dimension = vector.getDimension();
    return vector instanceof SparseRealVector ? sparse(dimension) : dense(dimension);
  }

  // Not instantiated
  private Vectors() {}
}
