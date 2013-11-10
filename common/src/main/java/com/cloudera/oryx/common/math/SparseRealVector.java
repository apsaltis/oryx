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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.oryx.common.math;

import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.RealVectorChangingVisitor;
import org.apache.commons.math3.linear.RealVectorPreservingVisitor;

import java.util.Iterator;

/**
 * Rescued from Commons Math 3.2.
 */
public abstract class SparseRealVector extends RealVector {

  public abstract Iterator<Entry> sparseInOrderIterator();

  @Override
  public final double walkInDefaultOrder(RealVectorPreservingVisitor visitor) {
    return doWalk(sparseInOrderIterator(), visitor);
  }

  @Override
  public final double walkInDefaultOrder(RealVectorPreservingVisitor visitor, int start, int end) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("deprecation")
  @Override
  public final double walkInOptimizedOrder(RealVectorPreservingVisitor visitor) {
    return doWalk(sparseIterator(), visitor);
  }

  @Override
  public final double walkInOptimizedOrder(RealVectorPreservingVisitor visitor, int start, int end) {
    throw new UnsupportedOperationException();
  }

  private double doWalk(Iterator<Entry> it, RealVectorPreservingVisitor visitor) {
    int dim = getDimension();
    visitor.start(dim, 0, dim - 1);
    while (it.hasNext()) {
      Entry e = it.next();
      visitor.visit(e.getIndex(), e.getValue());
    }
    return visitor.end();
  }

  @Override
  public final double walkInDefaultOrder(RealVectorChangingVisitor visitor) {
    return doWalk(sparseInOrderIterator(), visitor);
  }

  @Override
  public final double walkInDefaultOrder(RealVectorChangingVisitor visitor, int start, int end) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("deprecation")
  @Override
  public final double walkInOptimizedOrder(RealVectorChangingVisitor visitor) {
    return doWalk(sparseIterator(), visitor);
  }

  @Override
  public final double walkInOptimizedOrder(RealVectorChangingVisitor visitor, int start, int end) {
    throw new UnsupportedOperationException();
  }

  private double doWalk(Iterator<Entry> it, RealVectorChangingVisitor visitor) {
    int dim = getDimension();
    visitor.start(dim, 0, dim - 1);
    while (it.hasNext()) {
      Entry e = it.next();
      e.setValue(visitor.visit(e.getIndex(), e.getValue()));
    }
    return visitor.end();
  }

  // Added for Oryx

  /**
   * @return number of entries which the vector has actually set to some value
   */
  public abstract int getNumEntries();

}