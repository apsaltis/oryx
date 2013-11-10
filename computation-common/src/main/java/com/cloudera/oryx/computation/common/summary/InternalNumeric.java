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

final class InternalNumeric {
  private double min = Double.POSITIVE_INFINITY;
  private double max = Double.NEGATIVE_INFINITY;
  private double sum;
  private double sumSq;
  private long missing;

  public Numeric toNumeric(long recordCount) {
    if (missing == recordCount) {
      return new Numeric(0.0, 0.0, 0.0, 0.0, missing);
    }
    long n = recordCount - missing;
    double mean = sum / n;
    double stdDev = Math.sqrt((sumSq / n) - mean * mean);
    return new Numeric(min, max, mean, stdDev, missing);
  }
  
  public void update(double d) {
    if (Double.isNaN(d)) {
      missing++;
    } else {
      sum += d;
      sumSq += d * d;
      if (d < min) {
        min = d;
      }
      if (d > max) {
        max = d;
      }
    }
  }
  
  public void merge(InternalNumeric other) {
    sum += other.sum;
    sumSq += other.sumSq;
    missing += other.missing;
    if (other.min < min) {
      min = other.min;
    }
    if (other.max > max) {
      max = other.max;
    }
  }
}