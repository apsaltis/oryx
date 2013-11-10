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

package com.cloudera.oryx.common.collection;

import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.random.RandomManager;

/**
 * Tests {@link SamplingLongPrimitiveIterator}.
 *
 * @author Sean Owen
 */
public final class SamplingLongPrimitiveIteratorTest extends OryxTest {

  @Test(expected = IllegalArgumentException.class)
  public void testSamplingRate1() {
    RandomGenerator random = RandomManager.getRandom();
    new SamplingLongPrimitiveIterator(random, new LongSet().iterator(), 1.1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSamplingRate2() {
    RandomGenerator random = RandomManager.getRandom();
    new SamplingLongPrimitiveIterator(random, new LongSet().iterator(), 0.0);
  }

  @Test
  public void testEmpty() {
    RandomGenerator random = RandomManager.getRandom();
    LongSet empty = new LongSet();
    LongPrimitiveIterator sampling = new SamplingLongPrimitiveIterator(random, empty.iterator(), 1.0);
    assertFalse(sampling.hasNext());
    try {
      sampling.nextLong();
      fail();
    } catch (NoSuchElementException nsee) {
      // good
    }
    sampling.skip(0);
    assertFalse(sampling.hasNext());
    sampling.skip(1);
    assertFalse(sampling.hasNext());
  }

  @Test
  public void testSample1() {
    RandomGenerator random = RandomManager.getRandom();
    LongSet set = new LongSet();
    set.addAll(new long[] {1,2,4,7,15});
    LongPrimitiveIterator sampling = new SamplingLongPrimitiveIterator(random, set.iterator(), 1.0);
    for (long l : new long[] {1,2,4,7,15}) {
      assertTrue(sampling.hasNext());
      assertEquals(l, sampling.nextLong());
    }
  }

  @Test
  public void testSample2() {
    RandomGenerator random = RandomManager.getRandom();
    LongSet set = new LongSet();
    set.addAll(new long[] {1,2,4,7,15});
    LongPrimitiveIterator sampling = new SamplingLongPrimitiveIterator(random, set.iterator(), 0.3);
    for (long l : new long[] {4,7}) {
      assertTrue(sampling.hasNext());
      assertEquals(l, sampling.nextLong());
    }
  }

  @Test
  public void testSample3() {
    RandomGenerator random = RandomManager.getRandom();
    LongSet set = new LongSet();
    set.addAll(new long[] {1,2,4,7,15});
    Iterator<?> sampling = new SamplingLongPrimitiveIterator(random, set.iterator(), 0.0001);
    assertFalse(sampling.hasNext());
  }

  @Test
  public void testSkip1() {
    RandomGenerator random = RandomManager.getRandom();
    LongSet set = new LongSet();
    set.addAll(new long[] {1,2,4,7,15});
    LongPrimitiveIterator sampling = new SamplingLongPrimitiveIterator(random, set.iterator(), 0.3);
    sampling.skip(0);
    assertTrue(sampling.hasNext());
    assertEquals(4, sampling.nextLong());
  }

  @Test
  public void testSkip2() {
    RandomGenerator random = RandomManager.getRandom();
    LongSet set = new LongSet();
    set.addAll(new long[] {1,2,4,7,15});
    LongPrimitiveIterator sampling = new SamplingLongPrimitiveIterator(random, set.iterator(), 0.3);
    sampling.skip(1);
    assertTrue(sampling.hasNext());
    assertEquals(7, sampling.nextLong());
  }

}
