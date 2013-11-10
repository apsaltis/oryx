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

package com.cloudera.oryx.common.stats;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link JVMEnvironment}.
 *
 * @author Sean Owen
 */
public final class JVMEnvironmentTest extends OryxTest {

  @Test
  public void testJVM() {
    JVMEnvironment env = new JVMEnvironment();
    assertNotNull(env.getHostName());
    assertTrue(env.getMaxMemory() > 1000000);
    assertTrue(env.getMaxMemoryMB() > 1);
    assertTrue(env.getUsedMemory() > 1000000);
    assertTrue(env.getUsedMemoryMB() > 1);
    assertTrue(env.getUsedMemory() < env.getMaxMemory());
    assertTrue(env.getUsedMemoryMB() < env.getMaxMemoryMB());
    assertTrue(env.getPercentUsedMemory() >= 0);
    assertTrue(env.getPercentUsedMemory() <= 100);
  }

}
