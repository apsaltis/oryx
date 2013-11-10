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

package com.cloudera.oryx.computation.common.crossfold;

import java.io.Serializable;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.random.RandomManager;

/**
 * Supports creating partitions of {@code PCollection}s for performing
 * cross-validations.
 */
public final class Crossfold implements Serializable {
  /**
   * The default seed allows us to partition an identical dataset the
   * same way on every pass over it, even from different Crunch jobs.
   */
  private static final long DEFAULT_SEED = 1729L;
  
  private final int numFolds;
  private final long seed;
  
  public Crossfold(int numFolds) {
    this(numFolds, DEFAULT_SEED);
  }
  
  public Crossfold(int numFolds, long seed) {
    Preconditions.checkArgument(numFolds > 0, "Number of folds must be greater than zero");
    this.numFolds = numFolds;
    this.seed = seed;
  }
  
  public int getNumFolds() {
    return numFolds;
  }

  public RandomGenerator getRandomGenerator() {
    return RandomManager.getSeededRandom(seed);
  }

  public <T> PCollection<Pair<Integer, T>> apply(PCollection<T> pcollect) {
    PTypeFamily ptf = pcollect.getTypeFamily();
    PType<Pair<Integer, T>> pt = ptf.pairs(ptf.ints(), pcollect.getPType());
    return pcollect.parallelDo("crossfold", new MapFn<T, Pair<Integer, T>>() {
      private transient RandomGenerator rand;
      
      @Override
      public void initialize() {
        if (rand == null) {
          this.rand = RandomManager.getSeededRandom(seed);
        }
      }
      
      @Override
      public Pair<Integer, T> map(T t) {
        return Pair.of(rand.nextInt(numFolds), t);
      }
      
    }, pt);
  }
}
