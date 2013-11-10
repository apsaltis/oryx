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

package com.cloudera.oryx.als.common;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link ByValueAscComparator}.
 *
 * @author Sean Owen
 */
public final class ByValueAscComparatorTest extends OryxTest {

  @Test
  public void testCompare() {
    NumericIDValue a = new NumericIDValue(1L, 2.0f);
    NumericIDValue b = new NumericIDValue(5L, 1.0f);
    assertTrue(ByValueAscComparator.INSTANCE.compare(a, b) > 0);
    assertTrue(ByValueAscComparator.INSTANCE.compare(b, a) < 0);
    assertEquals(0, ByValueAscComparator.INSTANCE.compare(a, a));
  }

  @Test
  public void testSameValue() {
    NumericIDValue a = new NumericIDValue(1L, 2.0f);
    NumericIDValue b = new NumericIDValue(5L, 2.0f);
    assertTrue(ByValueAscComparator.INSTANCE.compare(a, b) > 0);
    assertTrue(ByValueAscComparator.INSTANCE.compare(b, a) < 0);
    assertEquals(0, ByValueAscComparator.INSTANCE.compare(a, a));
  }

}
