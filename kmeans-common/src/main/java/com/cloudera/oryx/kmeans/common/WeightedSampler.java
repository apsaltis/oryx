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

package com.cloudera.oryx.kmeans.common;

import org.apache.commons.math3.random.RandomGenerator;

import java.util.Arrays;
import java.util.List;

import com.cloudera.oryx.common.random.RandomManager;

public final class WeightedSampler<T, W extends Weighted<T>> {

  private final double[] cumulativeSum;
  private final List<W> things;
  private final RandomGenerator random;
  
  public WeightedSampler(List<W> things, RandomGenerator random) {
    this.things = things;
    this.cumulativeSum = new double[things.size() + 1];
    for (int i = 0; i < things.size(); i++) {
      cumulativeSum[i + 1] = cumulativeSum[i] + things.get(i).weight(); 
    }
    this.random = (random == null) ? RandomManager.getRandom() : random;
  }
 
  public T sample() {
    double offset = random.nextDouble() * cumulativeSum[cumulativeSum.length - 1];
    int next = Arrays.binarySearch(cumulativeSum, offset);
    Weighted<T> item = (next >= 0) ? things.get(next - 1) : things.get(-2 - next);
    return item.thing();
  }
}
