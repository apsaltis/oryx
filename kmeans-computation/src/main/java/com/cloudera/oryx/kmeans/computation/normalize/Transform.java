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

package com.cloudera.oryx.kmeans.computation.normalize;

import java.io.Serializable;

import com.cloudera.oryx.computation.common.summary.SummaryStats;

public abstract class Transform implements Serializable {
  
  public abstract double apply(double value, SummaryStats stats);

  public static Transform forName(String name) {
    if ("z".equalsIgnoreCase(name)) {
      return Z;
    }
    if ("linear".equalsIgnoreCase(name)) {
      return LINEAR;
    }
    if ("log".equalsIgnoreCase(name)) {
      return LOG;
    }
    if ("none".equalsIgnoreCase(name)) {
      return NONE;
    }
    throw new IllegalArgumentException("Did not recognize transform: " + name);
  }
  
  public static final Transform NONE = new Transform() {
    @Override
    public double apply(double value, SummaryStats stats) {
      return value;
    }
  };
  
  public static final Transform Z = new Transform() {
    @Override
    public double apply(double value, SummaryStats stats) {
      if (stats.stdDev() == 0.0) {
        return value;
      }
      return (value - stats.mean()) / stats.stdDev();
    }
  };
  
  public static final Transform LINEAR = new Transform() {
    @Override
    public double apply(double value, SummaryStats stats) {
      if (stats.range() == 0.0) {
        return value;
      }
      return (value - stats.min()) / stats.range();
    }
  };
  
  public static final Transform LOG = new Transform() {
    @Override
    public double apply(double value, SummaryStats stats) {
      return Math.log(value);
    }
  };
}
