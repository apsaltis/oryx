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
import com.cloudera.oryx.common.collection.LongObjectMap;

/**
 * Tests {@link StringLongMapping}.
 *
 * @author Sean Owen
 */
public final class StringLongMappingTest extends OryxTest {

  @Test
  public void testToLong() {
    assertEquals(-7363431537935844490L, StringLongMapping.toLong("foo"));
  }

  @Test
  public void testNumeric() {
    assertEquals(0, StringLongMapping.toLong("0"));
  }

  @Test
  public void testSpecialValue() {
    // not long min value
    assertEquals(-1448413612742796816L, StringLongMapping.toLong(Long.toString(Long.MIN_VALUE)));
  }

  @Test
  public void testToString() {
    StringLongMapping mapping = new StringLongMapping();
    long hash = mapping.add("foo");
    assertEquals("foo", mapping.toString(hash));
  }

  @Test
  public void testMapping() {
    StringLongMapping mapping = new StringLongMapping();
    long hash = mapping.add("foo");
    LongObjectMap<String> reverse = mapping.getReverseMapping();
    assertEquals(1, reverse.size());
    assertEquals("foo", reverse.get(hash));
  }

}
