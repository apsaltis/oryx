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

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Utilities that are generally used but not quite core storage operations as in {@link Store}.
 * 
 * @author Sean Owen
 */
public final class StoreUtils {

  private static final Logger log = LoggerFactory.getLogger(StoreUtils.class);

  private static final Splitter ON_DELIMITER = Splitter.on('/');

  private StoreUtils() {
  }

  /**
   * Lists all generation keys for a given instance. This skips any system directories, for example.
   * 
   * @param instanceDir instance directory from which to retrieve generations for
   * @return locations of all generation directories for the given instance
   */
  public static List<String> listGenerationsForInstance(String instanceDir) throws IOException {
    String prefix = Namespaces.getInstancePrefix(instanceDir);
    List<String> rawGenerations = Store.get().list(prefix, false);
    Iterator<String> it = rawGenerations.iterator();
    String sysPrefix = Namespaces.getSysPrefix(instanceDir);
    while (it.hasNext()) {
      if (it.next().startsWith(sysPrefix)) {
        it.remove();
      }
    }
    return rawGenerations;
  }

  public static long parseGenerationFromPrefix(CharSequence prefix) {
    try {
      return Long.parseLong(lastNonEmptyDelimited(prefix));
    } catch (NumberFormatException nfe) {
      log.error("Bad generation directory: {}", prefix);
      throw nfe;
    }
  }

  public static String lastNonEmptyDelimited(CharSequence path) {
    String result = null;
    for (String s : ON_DELIMITER.split(path)) {
      if (!s.isEmpty()) {
        result = s;
      }
    }
    return result;
  }
  
}
