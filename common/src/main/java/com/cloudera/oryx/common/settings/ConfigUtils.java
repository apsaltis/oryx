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

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

/**
 * Utilities for retrieving {@code Config} instances.
 */
public final class ConfigUtils {

  private static Config DEFAULT_CONFIG = null;

  /**
   * Returns the default {@code Config} object for this app, based on config in the JAR file
   * or otherwise specified to the library.
   */
  public static synchronized Config getDefaultConfig() {
    if (DEFAULT_CONFIG == null) {
      DEFAULT_CONFIG = ConfigFactory.load();
    }
    return DEFAULT_CONFIG;
  }

  /**
   * Loads default configuration including a .conf file as though it were supplied via -Dconfig.file=userConfig
   *
   * Do NOT use this from user code.  Only to be used in test code.
   */
  public static synchronized void loadUserConfig(String userConfig) {
    if (DEFAULT_CONFIG == null) {
      DEFAULT_CONFIG = ConfigFactory.load(userConfig);
    }
  }

  public static synchronized void overlayConfigOnDefault(File configFile) {
    if (configFile.exists()) {
      Preconditions.checkArgument(!configFile.isDirectory(), "Cannot handle directories of config files %s", configFile);
      DEFAULT_CONFIG = ConfigFactory.parseFileAnySyntax(configFile).resolve().withFallback(getDefaultConfig());
    }
  }

  public static synchronized void overlayConfigOnDefault(String config) {
    if (config != null) {
      DEFAULT_CONFIG = ConfigFactory.parseString(config).resolve().withFallback(getDefaultConfig());
    }
  }

  private ConfigUtils() {}
}
