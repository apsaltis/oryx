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

package com.cloudera.oryx.common.math;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.RealVector;

import java.io.Serializable;

/**
 * Decorates a {@link RealVector} with a {@link String} name.
 *
 * @author Sean Owen
 */
public final class NamedRealVector extends RealVector implements Serializable {

  private final RealVector delegate;
  private final String name;

  /**
   * @param delegate underlying {@link RealVector} to name
   * @param name name attached to the {@link RealVector}
   */
  public NamedRealVector(RealVector delegate, String name) {
    Preconditions.checkArgument(!(delegate instanceof NamedRealVector));
    this.delegate = delegate;
    this.name = name;
  }

  /**
   * @return underlying {@link RealVector} that is being named
   */
  public RealVector getDelegate() {
    return delegate;
  }

  /**
   * @return vector name
   */
  public String getName() {
    return name;
  }

  @Override
  public int getDimension() {
    return delegate.getDimension();
  }

  @Override
  public double getEntry(int index) {
    return delegate.getEntry(index);
  }

  @Override
  public void setEntry(int index, double value) {
    delegate.setEntry(index, value);
  }

  @Override
  public RealVector append(RealVector v) {
    return delegate.append(v);
  }

  @Override
  public RealVector append(double d) {
    return delegate.append(d);
  }

  @Override
  public RealVector getSubVector(int index, int n) {
    return delegate.getSubVector(index, n);
  }

  @Override
  public void setSubVector(int index, RealVector v) {
    delegate.setSubVector(index, v);
  }

  @Override
  public boolean isNaN() {
    return delegate.isNaN();
  }

  @Override
  public boolean isInfinite() {
    return delegate.isInfinite();
  }

  @Override
  public RealVector copy() {
    return delegate.copy();
  }

  @Deprecated
  @Override
  public RealVector ebeDivide(RealVector v) {
    return delegate.ebeDivide(v);
  }

  @Deprecated
  @Override
  public RealVector ebeMultiply(RealVector v) {
    return delegate.ebeMultiply(v);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NamedRealVector)) {
      return false;
    }
    NamedRealVector other = (NamedRealVector) o;
    return
        (delegate == null ? other.delegate == null : delegate.equals(other.delegate)) &&
        (name == null ? other.name == null : name.equals(other.name));

  }

  @Override
  public int hashCode() {
    int result = delegate != null ? delegate.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
