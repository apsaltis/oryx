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

import java.io.File;

/**
 * Common settings that define how and where and API is available.
 */
public final class APISettings {

  private static final String API_PATH = "api";

  private static final String HOST_STR_PARAM = "host";
  private static final String PORT_INT_PARAM = "port";
  private static final String SECURE_PORT_INT_PARAM = "secure-port";
  private static final String USER_NAME_STR_PARAM = "user-name";
  private static final String PASSWORD_STR_PARAM = "password";
  private static final String KEYSTORE_FILE_STR_PARAM = "keystore-file";
  private static final String KEYSTORE_PASSWORD_STR_PARAM = "keystore-password";

  private final String host;
  private final int port;
  private final int securePort;
  private final String userName;
  private final String password;
  private final File keystoreFile;
  private final String keystorePassword;

  /**
   * Creates a new {@code ConnectionSettings} instance using the given configuration
   * information.
   *
   * @param config The configuration information
   * @return A new instance
   */
  public static APISettings create(Config config) {
    if (config.hasPath(API_PATH)) {
      config = config.getConfig(API_PATH);
    }
    int port = checkPort(config.getInt(PORT_INT_PARAM));
    int securePort = checkPort(config.getInt(SECURE_PORT_INT_PARAM));
    String host = checkHost(config.getString(HOST_STR_PARAM));
    return new APISettings(host, port, securePort, config);
  }

  static String checkHost(String host) {
    Preconditions.checkArgument(host == null || (!host.startsWith("http://") && !host.startsWith("https://")),
        "host should not include a URI scheme: %s", host);
    return host;
  }

  static int checkPort(int port) {
    Preconditions.checkArgument(port > 0, "port must be positive: %s", port);
    return port;
  }

  private APISettings(String host, int port, int securePort, Config config) {
    this.host = host;
    this.port = port;
    this.securePort = securePort;
    this.userName = config.hasPath(USER_NAME_STR_PARAM) ? config.getString(USER_NAME_STR_PARAM) : null;
    this.password = config.hasPath(PASSWORD_STR_PARAM) ? config.getString(PASSWORD_STR_PARAM) : null;
    if (config.hasPath(KEYSTORE_FILE_STR_PARAM)) {
      this.keystoreFile = new File(config.getString(KEYSTORE_FILE_STR_PARAM));
      this.keystorePassword = config.getString(KEYSTORE_PASSWORD_STR_PARAM);
    } else {
      this.keystoreFile = null;
      this.keystorePassword = null;
    }
  }

  /**
   * @return host containing the server.
   */
  public String getHost() {
    return host;
  }

  /**
   * @return port on which to access the server.
   */
  public int getPort() {
    return port;
  }

  /**
   * @return secure port on which to access the server.
   */
  public int getSecurePort() {
    return securePort;
  }

  /**
   * @return if true, this client is accessing the server over HTTPS, not HTTP
   */
  public boolean isSecure() {
    return keystoreFile != null;
  }

  /**
   * @return the keystore file containing the server's SSL keys. Only necessary when accessing a server with a
   *  temporary self-signed key, which is not by default trusted by the Java SSL implementation
   */
  public File getKeystoreFile() {
    return keystoreFile;
  }

  /**
   * @return password for {@link #getKeystoreFile()}
   */
  public String getKeystorePassword() {
    return keystorePassword;
  }

  /**
   * @return user name needed to access the Serving Layer, if any
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @return password needed to access the Serving Layer, if any
   */
  public String getPassword() {
    return password;
  }
}
