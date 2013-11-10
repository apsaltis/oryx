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

package com.cloudera.oryx.computation.common.sample;

import java.util.Map;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.random.RandomManager;

/**
 * Tests {@link ReservoirSampling}.
 *
 * @author Sean Owen
 */
public final class ReservoirSamplingTest extends OryxTest {

  private static final PCollection<Pair<String, Double>> VALUES = MemPipeline.typedCollectionOf(
      Writables.pairs(Writables.strings(), Writables.doubles()),
      ImmutableList.of(
        Pair.of("foo", 200.0),
        Pair.of("bar", 400.0),
        Pair.of("baz", 100.0),
        Pair.of("biz", 100.0)));
  
  @Test
  public void testWRS() throws Exception {
    Map<String, Integer> histogram = Maps.newHashMap();

    RandomGenerator r = RandomManager.getRandom();
    for (int i = 0; i < 100; i++) {
      PCollection<String> sample = ReservoirSampling.weightedSample(VALUES, 2, r);
      for (String s : sample.materialize()) {
        if (histogram.containsKey(s)) {
          histogram.put(s, 1 + histogram.get(s));
        } else {
          histogram.put(s, 1);
        }
      }
    }
    
    Map<String, Integer> expected = ImmutableMap.of(
        "foo", 48, "bar", 80, "baz", 32, "biz", 40);
    assertEquals(expected, histogram);
  }
}
