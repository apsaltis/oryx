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

import com.google.common.base.Preconditions;

/**
 * Based on Welford's algorithm (see http://www.johndcook.com/standard_deviation.html) for numeric stability.
 * We'd use Commons Math3 {@code Variance}, except that it doesn't allow decrementing.
 *
 * @author Sean Owen
 */
public final class UpdatableVariance {

  private long n;
  private double mk;
  private double sk;

  public UpdatableVariance() {
    mk = Double.NaN;
    sk = Double.NaN;
  }

  /**
   * @param xk new datum to add to the variance computation
   */
  public void increment(double xk) {
    if (++n == 1) {
      mk = xk;
      sk = 0.0;
    } else {
      double diff = xk - mk;
      mk += diff / n;
      sk += diff * (xk - mk);
    }
  }

  /**
   * @param xk new datum to remove from the variance computation
   */
  public void decrement(double xk) {
    Preconditions.checkState(n > 0);
    if (--n == 0) {
      mk = Double.NaN;
      sk = Double.NaN;
    } else {
      double mkPlus1 = mk;
      mk = ((n+1) * mkPlus1 - xk) / n;
      sk -= (xk - mk) * (xk - mkPlus1);
    }
  }

  /**
   * @return number of data points in the variance computation
   */
  public long getN() {
    return n;
  }

  /**
   * @return variance of data points
   */
  public double getResult() {
    return n <= 1 ? Double.NaN : sk / (n - 1);
  }

  @Override
  public String toString() {
    return Double.toString(getResult());
  }

}
