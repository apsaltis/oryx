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
import org.apache.commons.math3.util.Pair;
import org.junit.Test;

import java.util.List;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests iterations over sparse {@link RealVector}s -- implemented through methods like
 * {@link RealVector#walkInDefaultOrder(org.apache.commons.math3.linear.RealVectorChangingVisitor)}.
 *
 * @author Sean Owen
 */
public final class SparseIteratorTest extends OryxTest {

  @Test
  public void testEmptyDefaultSparseIterator() {
    RealVector vector = Vectors.sparse(0);
    DummyVisitor visitor = new DummyVisitor();
    vector.walkInDefaultOrder(visitor);
    assertTrue(visitor.getSeenValues().isEmpty());
  }

  @Test
  public void testEmptyOptimizedSparseIterator() {
    RealVector vector = Vectors.sparse(0);
    DummyVisitor visitor = new DummyVisitor();
    vector.walkInOptimizedOrder(visitor);
    assertTrue(visitor.getSeenValues().isEmpty());
  }

  @Test
  public void testSingleDefaultSparseIterator() {
    RealVector vector = Vectors.sparse(1);
    vector.setEntry(0, 3.0);
    DummyVisitor visitor = new DummyVisitor();
    vector.walkInDefaultOrder(visitor);
    assertEquals(1, visitor.getSeenValues().size());
    Pair<Integer,Double> first = visitor.getSeenValues().get(0);
    assertEquals(0, first.getFirst().intValue());
    assertEquals(3.0, first.getSecond().doubleValue());
  }

  @Test
  public void testSingleOptimizedSparseIterator() {
    RealVector vector = Vectors.sparse(1);
    vector.setEntry(0, 3);
    DummyVisitor visitor = new DummyVisitor();
    vector.walkInOptimizedOrder(visitor);
    assertEquals(1, visitor.getSeenValues().size());
    Pair<Integer,Double> first = visitor.getSeenValues().get(0);
    assertEquals(0, first.getFirst().intValue());
    assertEquals(3.0, first.getSecond().doubleValue());
  }

  @Test
  public void testManyDefaultSparseIterator() {
    RealVector vector = Vectors.sparse(10, 3);
    vector.setEntry(3, 4.5);
    vector.setEntry(8, 0.0); // Won't show up
    vector.setEntry(9, 3.0);
    vector.setEntry(2, -7.0);
    DummyVisitor visitor = new DummyVisitor();
    vector.walkInDefaultOrder(visitor);
    List<Pair<Integer,Double>> seen = visitor.getSeenValues();
    assertEquals(3, seen.size());
    assertEquals(2, seen.get(0).getFirst().intValue());
    assertEquals(-7.0, seen.get(0).getSecond().doubleValue());
    assertEquals(3, seen.get(1).getFirst().intValue());
    assertEquals(4.5, seen.get(1).getSecond().doubleValue());
    assertEquals(9, seen.get(2).getFirst().intValue());
    assertEquals(3.0, seen.get(2).getSecond().doubleValue());
  }

  @Test
  public void testManyOptimizedSparseIterator() {
    RealVector vector = Vectors.sparse(10, 3);
    vector.setEntry(3, 4.5);
    vector.setEntry(8, 0.0); // Won't show up
    vector.setEntry(9, 3.0);
    vector.setEntry(2, -7.0);
    DummyVisitor visitor = new DummyVisitor();
    vector.walkInOptimizedOrder(visitor);
    List<Pair<Integer,Double>> seen = visitor.getSeenValues();
    assertEquals(3, seen.size());
    assertEquals(9, seen.get(0).getFirst().intValue());
    assertEquals(3.0, seen.get(0).getSecond().doubleValue());
    assertEquals(2, seen.get(1).getFirst().intValue());
    assertEquals(-7.0, seen.get(1).getSecond().doubleValue());
    assertEquals(3, seen.get(2).getFirst().intValue());
    assertEquals(4.5, seen.get(2).getSecond().doubleValue());
  }


}
