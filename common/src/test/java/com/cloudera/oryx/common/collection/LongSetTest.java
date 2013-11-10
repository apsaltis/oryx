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

import com.google.common.collect.Sets;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.random.RandomManager;

/** <p>Tests {@link LongSet}.</p>
 *
 * @author Sean Owen
 * @author Mahout
 */
public final class LongSetTest extends OryxTest {

  @Test
  public void testContainsAndAdd() {
    LongSet set = new LongSet();
    assertFalse(set.contains(1));
    set.add(1);
    assertTrue(set.contains(1));
  }

  @Test
  public void testRemove() {
    LongSet set = new LongSet();
    set.add(1);
    set.remove(1);
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    assertFalse(set.contains(1));
  }

  @Test
  public void testClear() {
    LongSet set = new LongSet();
    set.add(1);
    set.clear();
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    assertFalse(set.contains(1));
  }

  @Test
  public void testSizeEmpty() {
    LongSet set = new LongSet();
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    set.add(1);
    assertEquals(1, set.size());
    assertFalse(set.isEmpty());
    set.remove(1);
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
  }

  @Test
  public void testContains() {
    LongSet set = buildTestFastSet();
    assertTrue(set.contains(1));
    assertTrue(set.contains(2));
    assertTrue(set.contains(3));
    assertFalse(set.contains(4));
  }

  @Test
  public void testReservedValues() {
    LongSet set = new LongSet();
    try {
      set.add(Long.MIN_VALUE);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // good
    }
    assertFalse(set.contains(Long.MIN_VALUE));
    try {
      set.add(Long.MAX_VALUE);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // good
    }
    assertFalse(set.contains(Long.MAX_VALUE));
  }

  @Test
  public void testRehash() {
    LongSet set = buildTestFastSet();
    set.remove(1);
    set.rehash();
    assertFalse(set.contains(1));
  }

  @Test
  public void testGrow() {
    LongSet set = new LongSet(1);
    set.add(1);
    set.add(2);
    assertTrue(set.contains(1));
    assertTrue(set.contains(2));
  }

  @Test
  public void testIterator() {
    LongSet set = buildTestFastSet();
    Collection<Long> expected = new HashSet<Long>(3);
    expected.add(1L);
    expected.add(2L);
    expected.add(3L);
    LongPrimitiveIterator it = set.iterator();
    while (it.hasNext()) {
      expected.remove(it.nextLong());
    }
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testVersusHashSet() {
    LongSet actual = new LongSet(1);
    Collection<Integer> expected = Sets.newHashSet();
    RandomGenerator r = RandomManager.getRandom();
    for (int i = 0; i < 1000000; i++) {
      double d = r.nextDouble();
      Integer key = r.nextInt(100);
      if (d < 0.4) {
        assertEquals(expected.contains(key), actual.contains(key));
      } else {
        if (d < 0.7) {
          assertEquals(expected.add(key), actual.add(key));
        } else {
          assertEquals(expected.remove(key), actual.remove(key));
        }
        assertEquals(expected.size(), actual.size());
        assertEquals(expected.isEmpty(), actual.isEmpty());
      }
    }
  }

  private static LongSet buildTestFastSet() {
    LongSet set = new LongSet();
    set.add(1);
    set.add(2);
    set.add(3);
    return set;
  }


}