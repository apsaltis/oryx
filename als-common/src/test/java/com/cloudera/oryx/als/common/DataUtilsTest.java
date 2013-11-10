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
 * Tests {@link DataUtils}.
 *
 * @author Sean Owen
 */
public final class DataUtilsTest extends OryxTest {

  @Test
  public void testParse() {
    assertArrayEquals(new float[] {2.0f,3.0f}, DataUtils.readFeatureVector("2,3"));
    assertArrayEquals(new float[] {2.0f,-3.0f}, DataUtils.readFeatureVector("2, -3"));
    assertArrayEquals(new float[] {0.4f}, DataUtils.readFeatureVector("0.4"));
    assertArrayEquals(new float[] {0.0f,-2.1f,0.0f,3.0f,0.0f}, DataUtils.readFeatureVector("0,-2.1,0,3,0"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBad() {
    DataUtils.readFeatureVector("NaN");
  }

}
