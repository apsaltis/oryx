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

package com.cloudera.oryx.kmeans.computation.covariance;

import org.apache.commons.lang.builder.HashCodeBuilder;

public final class CoMoment {

  private double mx;
  private double my;
  private long n;
  private double C;

  public CoMoment() {
    this(0.0, 0.0, 0L, 0.0);
  }

  public CoMoment(double mx, double my, long n, double C) {
    this.mx = mx;
    this.my = my;
    this.n = n;
    this.C = C;
  }

  public long getN() {
    return n;
  }

  public double getCovariance() {
    return C / n;
  }

  public double getMeanX() {
    return mx;
  }

  public void update(double x, double y) {
    n++;
    mx += (x - mx) / n;
    C += (x - mx) * (y - my); // yep, this is right.
    my += (y - my) / n;
  }

  public CoMoment merge(CoMoment cm) {
    long n = this.n + cm.n;
    double C = this.C + cm.C + (this.mx - cm.mx) * (this.my - cm.my) * this.n * cm.n / n;
    double mx = (this.n * this.mx + cm.n * cm.mx) / n;
    double my = (this.n * this.my + cm.n * cm.my) / n;
    return new CoMoment(mx, my, n, C);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(n)
        .append(C)
        .append(my)
        .append(mx)
        .toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CoMoment)) {
      return false;
    }
    CoMoment cm = (CoMoment) other;
    return n == cm.n && approx(C, cm.C) && approx(mx, cm.mx) && approx(my, cm.my);
  }

  private static boolean approx(double a, double b) {
    return Math.abs(a - b) < 1.0e-6;
  }

  @Override
  public String toString() {
    return "(" + n + ',' + C / n + ')';
  }
}
