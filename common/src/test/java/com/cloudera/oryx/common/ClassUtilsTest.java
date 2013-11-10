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

package com.cloudera.oryx.common;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Tests {@link ClassUtils}.
 *
 * @author Sean Owen
 */
public final class ClassUtilsTest extends OryxTest {

  @Test
  public void testInstantiate() {
    Set<?> set = ClassUtils.loadInstanceOf(HashSet.class.getName(), Set.class);
    assertTrue(set instanceof HashSet);
  }

  @Test
  public void testInstantiateWithArgs() {
    Number n = ClassUtils.loadInstanceOf(Integer.class.getName(),
        Number.class,
        new Class<?>[]{int.class},
        new Object[]{3});
    assertEquals(3, n.intValue());
  }

}
