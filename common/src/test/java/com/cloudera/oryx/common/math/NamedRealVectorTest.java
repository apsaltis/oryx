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

package com.cloudera.oryx.common.math;

import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link NamedRealVector}.
 *
 * @author Sean Owen
 */
public final class NamedRealVectorTest extends OryxTest {

  @Test
  public void testName() {
    RealVector vec = Vectors.of(1.0, 2.0);
    NamedRealVector named = new NamedRealVector(vec, "foo");
    assertEquals("foo", named.getName());
    assertEquals(1.0, named.getEntry(0));
    assertEquals(2, named.getDimension());
  }

}
