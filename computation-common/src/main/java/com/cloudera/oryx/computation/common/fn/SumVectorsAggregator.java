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

package com.cloudera.oryx.computation.common.fn;

import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.Aggregator;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;

/**
 * An aggregator that handles the common operation of summing up a large number
 * of {@code Vector} objects and their counts so that they may be averaged.
 */
public final class SumVectorsAggregator<V extends RealVector> implements Aggregator<Pair<V, Long>> {

  private transient RealVector sum;
  private long count;
  
  @Override
  public void initialize(Configuration conf) {
    reset();
  }

  @Override
  public void reset() {
    sum = null;
    count = 0L;
  }

  @Override
  public Iterable<Pair<V, Long>> results() {
    @SuppressWarnings("unchecked")
    Iterable<Pair<V, Long>> result = ImmutableList.of(Pair.of((V) sum, count));
    return result;
  }

  @Override
  public void update(Pair<V, Long> in) {
    if (sum == null) {
      sum = in.first().copy();
    } else {
      sum = sum.add(in.first());
    }
    count += in.second();
  }
}
