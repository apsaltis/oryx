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

package com.cloudera.oryx.common.servcomp;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Represents resource names on HDFS.
 * 
 * @author Sean Owen
 */
public final class Namespaces {

  private static final Namespaces instance = new Namespaces();

  private final String prefix;

  private Namespaces() {
    if (ConfigUtils.getDefaultConfig().getBoolean("model.local")) {
      prefix = "file:";
    } else {
      URI defaultURI = FileSystem.getDefaultUri(new OryxConfiguration());
      String host = defaultURI.getHost();
      int port = defaultURI.getPort();
      if (port > 0) {
        prefix = "hdfs://" + host + ':' + port;
      } else {
        prefix = "hdfs://" + host;
      }
    }
  }

  /**
   * @return singleton {@code Namespaces} instance
   */
  public static Namespaces get() {
    return instance;
  }

  public String getPrefix() {
    return prefix;
  }

  public static String getSysPrefix(String instanceDir) {
    return getInstancePrefix(instanceDir) + "sys/";
  }

  /**
   * @return key where remote keystore file is optionally stored
   */
  public static String getKeystoreFilePrefix(String instanceDir) {
    return getSysPrefix(instanceDir) + "keystore.ks";
  }

  /**
   * @param suffix directory name
   * @return {@link Path} appropriate for use with Hadoop representing this directory
   */
  public static Path toPath(String suffix) {
    return new Path(get().getPrefix() + suffix);
  }

  public static String getInstancePrefix(String instanceDir) {
    return instanceDir + '/';
  }

  public static String getInstanceGenerationPrefix(String instanceDir, long generationID) {
    Preconditions.checkArgument(generationID >= 0L, "Bad generation %s", generationID);
    return getInstancePrefix(instanceDir) + getPaddedGenerationID(generationID) + '/';
  }

  private static String getPaddedGenerationID(long generationID) {
    return Strings.padStart(Long.toString(generationID), 5, '0');
  }

  public static String getTempPrefix(String instanceDir, long generationID) {
    return getInstanceGenerationPrefix(instanceDir, generationID) + "tmp/";
  }

  public static String getIterationsPrefix(String instanceDir, long generationID) {
    return getTempPrefix(instanceDir, generationID) + "iterations/";
  }

  public static String getGenerationDoneKey(String instanceDir, long generationID) {
    return getInstanceGenerationPrefix(instanceDir, generationID) + "_SUCCESS";
  }

}
