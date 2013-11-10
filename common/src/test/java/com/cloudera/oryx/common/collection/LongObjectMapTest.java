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

import com.cloudera.oryx.common.OryxTest;
import com.google.common.collect.Maps;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;

import java.util.Map;

import com.cloudera.oryx.common.random.RandomManager;

/**
 * @author Sean Owen
 * @author Mahout
 */
public final class LongObjectMapTest extends OryxTest {

  @Test
  public void testPutAndGet() {
    LongObjectMap<Long> map = new LongObjectMap<Long>();
    assertNull(map.get(500000L));
    map.put(500000L, 2L);
    assertEquals(2L, (long) map.get(500000L));
  }
  
  @Test
  public void testRemove() {
    LongObjectMap<Long> map = new LongObjectMap<Long>();
    map.put(500000L, 2L);
    map.remove(500000L);
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
    assertNull(map.get(500000L));
  }
  
  @Test
  public void testClear() {
    LongObjectMap<Long> map = new LongObjectMap<Long>();
    map.put(500000L, 2L);
    map.clear();
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
    assertNull(map.get(500000L));
  }
  
  @Test
  public void testSizeEmpty() {
    LongObjectMap<Long> map = new LongObjectMap<Long>();
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
    map.put(500000L, 2L);
    assertEquals(1, map.size());
    assertFalse(map.isEmpty());
    map.remove(500000L);
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
  }
  
  @Test
  public void testContains() {
    LongObjectMap<String> map = buildTestFastMap();
    assertTrue(map.containsKey(500000L));
    assertTrue(map.containsKey(47L));
    assertTrue(map.containsKey(2L));
    assertTrue(map.containsValue("alpha"));
    assertTrue(map.containsValue("bang"));
    assertTrue(map.containsValue("beta"));
    assertFalse(map.containsKey(999));
    assertFalse(map.containsValue("something"));
  }

  @Test
  public void testRehash() {
    LongObjectMap<String> map = buildTestFastMap();
    map.remove(500000L);
    map.rehash();
    assertNull(map.get(500000L));
    assertEquals("bang", map.get(47L));
  }
   
  @Test
  public void testVersusHashMap() {
    LongObjectMap<String> actual = new LongObjectMap<String>();
    Map<Long, String> expected = Maps.newHashMap();
    RandomGenerator r = RandomManager.getRandom();
    for (int i = 0; i < 1000000; i++) {
      double d = r.nextDouble();
      Long key = (long) r.nextInt(100);
      if (d < 0.4) {
        assertEquals(expected.get(key), actual.get(key));
      } else {
        if (d < 0.7) {
          assertEquals(expected.put(key, "bang"), actual.put(key, "bang"));
        } else {
          assertEquals(expected.remove(key), actual.remove(key));
        }
        assertEquals(expected.size(), actual.size());
        assertEquals(expected.isEmpty(), actual.isEmpty());
      }
    }
  }
  
  @Test
  public void testMaxSize() {
    LongObjectMap<String> map = new LongObjectMap<String>();
    map.put(4, "bang");
    assertEquals(1, map.size());
    map.put(47L, "bang");
    assertEquals(2, map.size());
    assertNull(map.get(500000L));
    map.put(47L, "buzz");
    assertEquals(2, map.size());
    assertEquals("buzz", map.get(47L));
  }
  
  
  private static LongObjectMap<String> buildTestFastMap() {
    LongObjectMap<String> map = new LongObjectMap<String>();
    map.put(500000L, "alpha");
    map.put(47L, "bang");
    map.put(2L, "beta");
    return map;
  }
  
}
