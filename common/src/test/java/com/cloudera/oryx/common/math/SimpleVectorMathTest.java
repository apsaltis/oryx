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

import com.cloudera.oryx.common.OryxTest;
import org.junit.Test;

/**
 * Tests {@link SimpleVectorMath}.
 *
 * @author Sean Owen
 */
public final class SimpleVectorMathTest extends OryxTest {
  
  private static final float[] VEC1 = {-1.0f, 2.5f, 3.0f};
  private static final float[] VEC2 = {1.5f, -1.5f, 0.0f};

  @Test
  public void testDot() {
    assertEquals(-5.25, SimpleVectorMath.dot(VEC1, VEC2));
  }

  @Test
  public void testNorm() {
    assertEquals(4.03112887414928, SimpleVectorMath.norm(VEC1));
    assertEquals(2.12132034355964, SimpleVectorMath.norm(VEC2));
  }

  @Test
  public void testNormalize() {
    float[] vec = {-1.0f, 2.5f, 3.0f};
    SimpleVectorMath.normalize(vec);
    assertEquals(1.0f, (float) SimpleVectorMath.norm(vec));
    assertEquals((float) SimpleVectorMath.norm(VEC1), (float) SimpleVectorMath.dot(vec, VEC1));
  }

}
