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

package com.cloudera.oryx.rdf.common.information;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link Information}.
 *
 * @author Sean Owen
 */
public final class InformationTest extends OryxTest {

  @Test
  public void testEmptyInformation() {
    assertNaN(Information.entropy(new int[] {}));
  }

  @Test
  public void testEmptyClassInformation() {
    assertNaN(Information.entropy(new int[] {0}));
  }

  @Test
  public void testOneClassInformation() {
    assertEquals(0.0, Information.entropy(new int[] {1}));
  }

  @Test
  public void testTwoEqualClassInformation() {
    assertEquals(Math.log(2.0), Information.entropy(new int[] {1, 1}));
  }

  @Test
  public void testTwoUnequalClassInformation() {
    assertEquals(0.5623351446188083, Information.entropy(new int[] {2, 6}));
  }

}
