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

import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.random.RandomManager;

public final class WeightedTest extends OryxTest {

  private static final class ThingFn<T> implements Function<Weighted<T>, T> {
    @Override
    public T apply(Weighted<T> arg0) {
      return arg0.thing();
    }
  }
  
  @Test
  public void testBasic() throws Exception {
    Collection<Weighted<Integer>> things = Lists.newArrayList();
    RandomGenerator rand = RandomManager.getRandom();
    for (int i = 0; i < 50; i++) {
      things.add(new Weighted<Integer>(i, rand.nextDouble()));
    }
    List<Weighted<Integer>> s = Weighted.sample(things, 5, rand);
    assertEquals(ImmutableList.of(31, 46, 49, 23, 14), Lists.transform(s, new ThingFn<Integer>()));
  }
  
  @Test
  public void testEmpty() throws Exception {
    RandomGenerator rand = RandomManager.getRandom();
    assertEquals(0, Weighted.sample(ImmutableList.<Weighted<Long>>of(), 10, rand).size());
  }
  
  @Test
  public void testSmallerCollectionThanSize() throws Exception {
    RandomGenerator rand = RandomManager.getRandom();
    Collection<Weighted<Integer>> things = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      things.add(new Weighted<Integer>(i, rand.nextDouble()));
    }
    List<Weighted<Integer>> s = Weighted.sample(things, 10, rand);
    assertEquals(ImmutableList.of(3, 4, 1, 0, 2), Lists.transform(s, new ThingFn<Integer>()));
  }
}
