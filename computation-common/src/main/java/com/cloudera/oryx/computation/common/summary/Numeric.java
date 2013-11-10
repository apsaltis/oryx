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

package com.cloudera.oryx.computation.common.summary;

import java.io.Serializable;

public final class Numeric implements Serializable {

  private double min;
  private double max;
  private double mean;
  private double stdDev;
  private Long missing;
  private String transform;
  
  // For serialization
  private Numeric() { }
  
  public Numeric(double min, double max, double mean, double stdDev, long missing) {
    this(min, max, mean, stdDev);
    if (missing > 0) {
      this.missing = missing;
    }
  }
  
  public Numeric(double min, double max, double mean, double stdDev) {
    this.min = min;
    this.max = max;
    this.mean = mean;
    this.stdDev = stdDev;
  }
  
  public double min() {
    return min;
  }
  
  public double max() {
    return max;
  }
  
  public double mean() {
    return mean;
  }
  
  public double stdDev() {
    return stdDev;
  }
  
  public double range() {
    return max - min;
  }
  
  public String getTransform() {
    return transform;
  }
  
  public long getMissing() {
    return missing == null ? 0L : missing;
  }
}
