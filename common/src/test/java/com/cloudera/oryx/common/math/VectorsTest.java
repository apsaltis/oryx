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

import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link Vectors}.
 *
 * @author Sean Owen
 */
public final class VectorsTest extends OryxTest {

  @Test
  public void testDense() {
    RealVector dense = Vectors.dense(10);
    assertEquals(10, dense.getDimension());
  }

  @Test
  public void testOf() {
    RealVector of = Vectors.of(1.0, -2.0, 7.0);
    assertEquals(3, of.getDimension());
    assertEquals(1.0, of.getEntry(0));
    assertEquals(-2.0, of.getEntry(1));
    assertEquals(7.0, of.getEntry(2));
  }

  @Test
  public void testLike() {
    RealVector of = Vectors.of(1.0, -2.0, 7.0);
    RealVector like = Vectors.like(of);
    assertEquals(of.getDimension(), like.getDimension());
    assertEquals(0.0, like.getEntry(0));
    assertSame(of.getClass(), like.getClass());
  }

  @Test
  public void testSparse() {
    RealVector sparse = Vectors.sparse(Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, sparse.getDimension());
    assertEquals(0.0, sparse.getEntry(0));
  }

}
