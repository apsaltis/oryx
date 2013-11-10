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

package com.cloudera.oryx.common.collection;

import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.PascalDistribution;
import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.iterator.AbstractLongPrimitiveIterator;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;

/**
 * Wraps a {@link LongPrimitiveIterator} and returns only some subset of the elements that it would,
 * as determined by a sampling rate parameter.
 * 
 * Adapted from the same class in Mahout 0.8.
 * 
 * @author Sean Owen
 * @author Mahout
 */
public final class SamplingLongPrimitiveIterator extends AbstractLongPrimitiveIterator {
  
  private final IntegerDistribution geometricDistribution;
  private final LongPrimitiveIterator delegate;
  private long next;
  private boolean hasNext;

  /**
   *
   * @param random random number generator to re-use
   * @param delegate iterator to sample from
   * @param samplingRate sampling rate in (0,1]
   */
  public SamplingLongPrimitiveIterator(RandomGenerator random, LongPrimitiveIterator delegate, double samplingRate) {
    Preconditions.checkNotNull(random);
    Preconditions.checkNotNull(delegate);
    Preconditions.checkArgument(samplingRate > 0.0 && samplingRate <= 1.0);
    // Geometric distribution is special case of negative binomial (aka Pascal) with r=1:
    geometricDistribution = new PascalDistribution(random, 1, samplingRate);
    this.delegate = delegate;
    this.hasNext = true;
    doNext();
  }
  
  @Override
  public boolean hasNext() {
    return hasNext;
  }
  
  @Override
  public long nextLong() {
    if (hasNext) {
      long result = next;
      doNext();
      return result;
    }
    throw new NoSuchElementException();
  }
  
  private void doNext() {
    int toSkip = geometricDistribution.sample();
    delegate.skip(toSkip);
    if (delegate.hasNext()) {
      next = delegate.next();
    } else {
      hasNext = false;
    }
  }
  
  /**
   * @throws UnsupportedOperationException
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void skip(int n) {
    if (n < 1) {
      return;
    }
    int toSkip = 0;
    for (int i = 0; i < n; i++) {
      toSkip += geometricDistribution.sample();
    }
    delegate.skip(toSkip);
    if (delegate.hasNext()) {
      next = delegate.next();
    } else {
      hasNext = false;
    }
  }
  
}