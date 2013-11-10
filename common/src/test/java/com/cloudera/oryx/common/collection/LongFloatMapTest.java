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

import java.util.Map;

import com.cloudera.oryx.common.OryxTest;
import com.google.common.collect.Maps;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;

import com.cloudera.oryx.common.random.RandomManager;

/**
 * @author Sean Owen
 * @author Mahout
 */
public final class LongFloatMapTest extends OryxTest {

  @Test
  public void testPutAndGet() {
    LongFloatMap map = new LongFloatMap();
    assertNaN(map.get(500000L));
    map.put(500000L, 2.0f);
    assertEquals(2.0f, map.get(500000L));
  }
  
  @Test
  public void testRemove() {
    LongFloatMap map = new LongFloatMap();
    map.put(500000L, 2.0f);
    map.remove(500000L);
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
    assertNaN(map.get(500000L));
  }
  
  @Test
  public void testClear() {
    LongFloatMap map = new LongFloatMap();
    map.put(500000L, 2.0f);
    map.clear();
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
    assertNaN(map.get(500000L));
  }
  
  @Test
  public void testSizeEmpty() {
    LongFloatMap map = new LongFloatMap();
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
    map.put(500000L, 2.0f);
    assertEquals(1, map.size());
    assertFalse(map.isEmpty());
    map.remove(500000L);
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
  }
  
  @Test
  public void testContains() {
    LongFloatMap map = buildTestFastMap();
    assertTrue(map.containsKey(500000L));
    assertTrue(map.containsKey(47L));
    assertTrue(map.containsKey(2L));
    assertFalse(map.containsKey(999));
  }

  @Test
  public void testRehash() {
    LongFloatMap map = buildTestFastMap();
    map.remove(500000L);
    map.rehash();
    assertNaN(map.get(500000L));
    assertEquals(3.0f, map.get(47L));
  }
   
  @Test
  public void testVersusHashMap() {
    LongFloatMap actual = new LongFloatMap();
    Map<Long,Float> expected = Maps.newHashMap();
    RandomGenerator r = RandomManager.getRandom();
    for (int i = 0; i < 1000000; i++) {
      double d = r.nextDouble();
      Long key = (long) r.nextInt(100);
      if (d < 0.4) {
        Number expectedValue = expected.get(key);
        float actualValue = actual.get(key);
        if (expectedValue == null) {
          assertNaN(actualValue);
        } else {
          assertEquals(expectedValue.floatValue(), actualValue);
        }
      } else {
        if (d < 0.7) {
          expected.put(key, 3.0f);
          actual.put(key, 3.0f);
        } else {
          expected.remove(key);
          actual.remove(key);
        }
        assertEquals(expected.size(), actual.size());
        assertEquals(expected.isEmpty(), actual.isEmpty());
      }
    }
  }
  
  @Test
  public void testMaxSize() {
    LongFloatMap map = new LongFloatMap();
    map.put(4, 3.0f);
    assertEquals(1, map.size());
    map.put(47L, 3.0f);
    assertEquals(2, map.size());
    assertNaN(map.get(500000L));
    map.put(47L, 5.0f);
    assertEquals(2, map.size());
    assertEquals(5.0f, map.get(47L));
  }
  
  
  private static LongFloatMap buildTestFastMap() {
    LongFloatMap map = new LongFloatMap();
    map.put(500000L, 2.0f);
    map.put(47L, 3.0f);
    map.put(2L, 5.0f);
    return map;
  }
  
}
