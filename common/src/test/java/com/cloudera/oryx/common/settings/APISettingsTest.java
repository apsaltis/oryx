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

package com.cloudera.oryx.common.settings;

import com.cloudera.oryx.common.OryxTest;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.junit.Test;

/**
 * Tests {@link APISettings}.
 *
 * @author Sean Owen
 */
public final class APISettingsTest extends OryxTest {

  @Test
  public void testHost() {
    String validHost = "example.com";
    assertEquals(validHost, APISettings.checkHost(validHost));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUriHost() {
    APISettings.checkHost("http://www.example.com");
  }

  @Test
  public void testPort() {
    int port = 1729;
    assertEquals(port, APISettings.checkPort(port));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPort() {
    APISettings.checkPort(0);
  }

  @Test(expected = ConfigException.Null.class)
  public void testDefaultConfig() {
    Config generic = ConfigUtils.getDefaultConfig().getConfig("generic-api");
    APISettings.create(generic);
  }

  @Test
  public void testServingLayerDefaults() {
    assertNotNull(APISettings.create(
        ConfigUtils.getDefaultConfig().getConfig("serving-layer.api")));
  }

  @Test
  public void testComputationLayerDefaults() {
    assertNotNull(APISettings.create(
        ConfigUtils.getDefaultConfig().getConfig("computation-layer.api")));
  }
}
