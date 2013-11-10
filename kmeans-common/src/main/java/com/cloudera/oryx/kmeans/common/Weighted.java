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

import java.util.Collection;
import java.util.List;
import java.util.SortedMap;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.LangUtils;
import com.cloudera.oryx.common.random.RandomManager;

/**
 * A typed container for an object and an associated numeric weight.
 */
public class Weighted<T> {

  private final T thing;
  private final double weight;
  
  private static final class WeightFunction<T> implements Function<T, Weighted<T>> {
    @Override
    public Weighted<T> apply(T input) {
      return new Weighted<T>(input);
    }
  }
  
  /**
   * Converts an input {@code Collection<T>} into a {@code Collection<Weighted>}.
   * 
   * @param things The items to convert
   * @return The items as {@code Weighted<T>} values with weight 1.0
   */
  public static <T> Collection<Weighted<T>> create(Collection<T> things) {
    return Collections2.transform(things, new WeightFunction<T>());
  }
  
  /**
   * Converts an input {@code List<T>} into a {@code List<Weighted>}.
   * 
   * @param things The items to convert
   * @return The items as {@code Weighted<T>} values with weight 1.0
   */
  public static <T> List<Weighted<T>> create(List<T> things) {
    return Lists.transform(things, new WeightFunction<T>());
  }
  
  /**
   * Sample items from a {@code List<Weighted>} where items with higher weights
   * have a higher probability of being included in the sample.
   * 
   * @param things The iterable to sample from
   * @param size The number of items to sample
   * @return A list containing the sampled items
   */
  public static <T extends Weighted<?>> List<T> sample(Iterable<T> things, int size, RandomGenerator random) {
    if (random == null) {
      random = RandomManager.getRandom();
    }
    SortedMap<Double, T> sampled = Maps.newTreeMap();
    for (T thing : things) {
      if (thing.weight() > 0) {
        double score = Math.log(random.nextDouble()) / thing.weight();
        if (sampled.size() < size || score > sampled.firstKey()) {
          sampled.put(score, thing);
        }
        if (sampled.size() > size) {
          sampled.remove(sampled.firstKey());
        }
      }
    }
    return Lists.newArrayList(sampled.values());
  }

  /**
   * Create a new instance with weight 1.0.
   * 
   * @param thing The thing that is weighted
   */
  public Weighted(T thing) {
    this(thing, 1.0);
  }
  
  /**
   * Create a new instance with the given weight.
   * 
   * @param thing The (non-null) thing to weight
   * @param weight The weight
   */
  public Weighted(T thing, double weight) {
    this.thing = Preconditions.checkNotNull(thing);
    this.weight = weight;
  }
  
  /**
   * Return the thing referenced by this instance.
   */
  public T thing() {
    return thing;
  }
  
  /**
   * Return the numeric weight for this instance.
   */
  public double weight() {
    return weight;
  }
  
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Weighted)) {
      return false;
    }
    Weighted<?> wv = (Weighted<?>) other;
    return thing.equals(wv.thing) && weight == wv.weight;
  }
  
  @Override
  public int hashCode() {
    return 17 * thing.hashCode() + 37 * LangUtils.hashDouble(weight);
  }
  
  @Override
  public String toString() {
    return String.valueOf(thing) + ';' + weight;
  }
}
