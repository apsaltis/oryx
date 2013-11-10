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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;

/**
 * General utility methods related to the language, or primitves.
 *
 * @author Sean Owen
 */
public final class LangUtils {

  private LangUtils() {
  }

  /**
   * Parses a {@code float} from a {@link String} as if by {@link Float#valueOf(String)}, but disallows special
   * values like {@link Float#NaN}, {@link Float#POSITIVE_INFINITY} and {@link Float#NEGATIVE_INFINITY}.
   *
   * @param s {@link String} to parse
   * @return floating-point value in the {@link String}
   * @throws NumberFormatException if input does not parse as a floating-point value
   * @throws IllegalArgumentException if input is infinite or {@link Float#NaN}
   * @see #parseDouble(String)
   */
  public static float parseFloat(String s) {
    float value = Float.parseFloat(s);
    Preconditions.checkArgument(Floats.isFinite(value), "Bad value: %s", value);
    return value;
  }

  /**
   * @see #parseFloat(String)
   */
  public static double parseDouble(String s) {
    double value = Double.parseDouble(s);
    Preconditions.checkArgument(Doubles.isFinite(value), "Bad value: %s", value);
    return value;
  }

  /**
   * Like {@link Doubles#hashCode(double)} but avoids creating a whole new object!
   * @return the same value produced by {@link Double#hashCode()}
   */
  public static int hashDouble(double d) {
    long bits = Double.doubleToLongBits(d);
    return (int) (bits ^ (bits >>> 32));
  }

}
