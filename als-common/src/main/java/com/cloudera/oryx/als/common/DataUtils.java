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

package com.cloudera.oryx.als.common;

import java.util.regex.Pattern;

import com.cloudera.oryx.common.LangUtils;

/**
 * Utility methods for dealing with "data", without any better home.
 *
 * @author Sean Owen
 */
public final class DataUtils {

  // For speed, use a pattern directly as we know it is not general CSV
  private static final Pattern COMMA = Pattern.compile(",");

  private DataUtils() {
  }

  /**
   * @param featureVectorString a feature vector as a comma-separated list of {@code float} values
   * @return those values parsed into a {@code float[]}
   */
  public static float[] readFeatureVector(CharSequence featureVectorString) {
    String[] elementTokens = COMMA.split(featureVectorString);
    int features = elementTokens.length;
    float[] elements = new float[features];
    for (int i = 0; i < features; i++) {
      elements[i] = LangUtils.parseFloat(elementTokens[i]);
    }
    return elements;
  }

}
