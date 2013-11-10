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

package com.cloudera.oryx.common.io;

import org.junit.Test;

import java.util.Arrays;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link DelimitedDataUtils}.
 *
 * @author Sean Owen
 */
public final class DelimitedDataUtilsTest extends OryxTest {

  @Test
  public void testEncode() {
    assertEquals("", DelimitedDataUtils.encode());
    assertEquals("foo", DelimitedDataUtils.encode("foo"));
    assertEquals("foo,bar", DelimitedDataUtils.encode("foo", "bar"));
  }

  @Test
  public void testEncodeDelimiters() {
    assertEquals("\"foo,bing\",bar", DelimitedDataUtils.encode("foo,bing", "bar"));
  }

  @Test
  public void testEncodeQuote() {
    assertEquals("foo,\"bar\"\"bing\"", DelimitedDataUtils.encode("foo", "bar\"bing"));
  }

  @Test
  public void testDecode() {
    assertArrayEquals(new String[0], DelimitedDataUtils.decode(""));
    assertArrayEquals(new String[] {"foo"}, DelimitedDataUtils.decode("foo"));
    assertArrayEquals(new String[] {"foo","bar"}, DelimitedDataUtils.decode("foo,bar"));
  }

  @Test
  public void testDecodeEmpty() {
    assertArrayEquals(new String[0], DelimitedDataUtils.decode(""));
    assertArrayEquals(new String[] {"foo", "bar", ""}, DelimitedDataUtils.decode("foo,bar,"));
    assertArrayEquals(new String[] {"", "foo", "bar"}, DelimitedDataUtils.decode(",foo,bar"));
    assertArrayEquals(new String[] {"foo", "", "bar"}, DelimitedDataUtils.decode("foo,,bar"));
  }

  @Test
  public void testDecodeDelimiters() {
    assertArrayEquals(new String[] {"foo,bing","bar"}, DelimitedDataUtils.decode("\"foo,bing\",bar"));
  }

  @Test
  public void testDecodeQuote() {
    assertArrayEquals(new String[] {"foo", "bar\"bing"}, DelimitedDataUtils.decode("foo,\"bar\"\"bing\""));
  }

  @Test
  public void testVsIterable() {
    assertEquals(DelimitedDataUtils.encode("foo", "bar"), DelimitedDataUtils.encode(Arrays.asList("foo", "bar")));
  }

  @Test
  public void testVsObject() {
    assertEquals(DelimitedDataUtils.encode("3.0", "-2.0"), DelimitedDataUtils.encode(Arrays.asList(3.0, -2.0)));
  }

}
