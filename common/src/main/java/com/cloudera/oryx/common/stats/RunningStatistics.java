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

package com.cloudera.oryx.common.stats;

import java.io.Serializable;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;

/**
 * Encapsulates a set of statistics like mean, min and max that can be updated, together, incrementally.
 *
 * @author Sean Owen
 */
public final class RunningStatistics implements Serializable {

  private final Mean mean;
  private final Min min;
  private final Max max;

  public RunningStatistics() {
    this.mean = new Mean();
    this.min = new Min();
    this.max = new Max();
  }

  /**
   * @return number of values that {@link #increment(double)} has been called on.
   */
  public long getCount() {
    return mean.getN();
  }

  /**
   * @return mean of all values that {@link #increment(double)} has been called with.
   */
  public double getMean() {
    return mean.getResult();
  }

  /**
   * @return minimum of all values passed to {@link #increment(double)}
   */
  public double getMin() {
    return min.getResult();
  }

  /**
   * @return maximum of all values passed to {@link #increment(double)}
   */
  public double getMax() {
    return max.getResult();
  }

  /**
   * @param value add a new value to the running statistics
   */
  public void increment(double value) {
    mean.increment(value);
    min.increment(value);
    max.increment(value);
  }

}
