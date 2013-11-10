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
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * Encapsulates a set of statistics, like those in {@link RunningStatistics}, but buckets the values by time.
 * For example, this can record mean/max/min values of a process per minute ({@link TimeUnit#MINUTES}. Over time,
 * old values will be 'removed' from the statistics. Updates happen passively, in response to calls to
 * {@link #refresh()} or {@link #increment(double)}.
 *
 * @author Sean Owen
 */
public final class RunningStatisticsPerTime implements Serializable {

  private final IntegerWeightedMean mean;
  private double min;
  private double max;
  private final long bucketTimeMS;
  private final Deque<RunningStatistics> subBuckets;
  private long frontBucketValidUntil;

  /**
   * @param timeUnit size of buckets, in time units, to use. Must be at least {@link TimeUnit#MINUTES}.
   */
  public RunningStatisticsPerTime(TimeUnit timeUnit) {
    int timeUnitOrdinal = timeUnit.ordinal();
    Preconditions.checkArgument(timeUnitOrdinal >= TimeUnit.MINUTES.ordinal(), "Unsupported time unit: %s", timeUnit);
    TimeUnit subTimeUnit = TimeUnit.values()[timeUnitOrdinal - 1];
    int numBuckets = (int) subTimeUnit.convert(1, timeUnit);

    mean = new IntegerWeightedMean();
    min = Double.NaN;
    max = Double.NaN;
    bucketTimeMS = TimeUnit.MILLISECONDS.convert(1, subTimeUnit);
    subBuckets = new LinkedList<RunningStatistics>();
    for (int i = 0; i < numBuckets; i++) {
      subBuckets.add(new RunningStatistics());
    }
    frontBucketValidUntil = System.currentTimeMillis() + bucketTimeMS;
  }

  /**
   * Manually refreshes internally computed values that are returned from {@link #getCount()} and similar methods
   * to reflect what the values should be "now".
   */
  public synchronized void refresh() {
    long now = System.currentTimeMillis();
    while (now > frontBucketValidUntil) {

      RunningStatistics removedBucket = subBuckets.removeLast();
      long count = removedBucket.getCount();
      if (count > 0) {
        mean.decrement(removedBucket.getMean(), count);
      }

      if (removedBucket.getMin() <= min) {
        double newMin = Double.NaN;
        for (RunningStatistics bucket : subBuckets) {
          double bucketMin = bucket.getMin();
          if (Double.isNaN(newMin) || bucketMin < newMin) {
            newMin = bucketMin;
          }
        }
        min = newMin;
      }
      if (removedBucket.getMax() >= max) {
        double newMax = Double.NaN;
        for (RunningStatistics bucket : subBuckets) {
          double bucketMax = bucket.getMax();
          if (Double.isNaN(newMax) || bucketMax > newMax) {
            newMax = bucketMax;
          }
        }
        max = newMax;
      }

      subBuckets.addFirst(new RunningStatistics());
      frontBucketValidUntil += bucketTimeMS;
    }
  }

  /**
   * @param value new data point to add, "now" into the current time bucket. Results returned from values like
   *  {@link #getCount()} will be updated in the process.
   */
  public synchronized void increment(double value) {
    refresh();
    mean.increment(value);
    subBuckets.getFirst().increment(value);
    if (Double.isNaN(min) || value < min) {
      min = value;
    }
    if (Double.isNaN(max) || value > max) {
      max = value;
    }
  }

  /**
   * @return number of data points in the time window. Note that this value will only be updated to reflect the
   *  current time on a call to {@link #increment(double)} or {@link #refresh()}
   */
  public synchronized long getCount() {
    return mean.getN();
  }

  /**
   * @return mean of data points the time window. Note that this value will only be updated to reflect the
   *  current time on a call to {@link #increment(double)} or {@link #refresh()}
   */
  public synchronized double getMean() {
    return mean.getResult();
  }

  /**
   * @return minimum data point in the time window. Note that this value will only be updated to reflect the
   *  current time on a call to {@link #increment(double)} or {@link #refresh()}
   */
  public synchronized double getMin() {
    return min;
  }

  /**
   * @return maximum data point in the time window. Note that this value will only be updated to reflect the
   *  current time on a call to {@link #increment(double)} or {@link #refresh()}
   */
  public synchronized double getMax() {
    return max;
  }

}
