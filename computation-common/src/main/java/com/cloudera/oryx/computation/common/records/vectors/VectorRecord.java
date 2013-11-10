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

package com.cloudera.oryx.computation.common.records.vectors;

import com.cloudera.oryx.common.math.Vectors;
import com.cloudera.oryx.computation.common.records.Record;

import org.apache.commons.math3.linear.RealVector;

import com.cloudera.oryx.computation.common.records.BasicSpec;
import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.Spec;

public final class VectorRecord implements Record {

  private final RealVector vector;
  
  public VectorRecord(RealVector vector) {
    this.vector = vector;
  }
  
  @Override
  public Spec getSpec() {
    return new BasicSpec(DataType.DOUBLE, vector.getDimension());
  }

  @Override
  public int size() {
    return vector.getDimension();
  }
  
  @Override
  public Record copy(boolean deep) {
    return new VectorRecord(deep ? vector.copy() : Vectors.like(vector));
  }

  public RealVector getVector() {
    return vector;
  }
  
  @Override
  public Object get(int index) {
    return vector.getEntry(index);
  }
  
  @Override
  public Boolean getBoolean(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Boolean getBoolean(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Double getDouble(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Double getDouble(int index) {
    return vector.getEntry(index);
  }

  @Override
  public Integer getInteger(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer getInteger(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Long getLong(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Long getLong(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Record set(int index, Object value) {
    vector.setEntry(index, ((Number) value).doubleValue());
    return this;
  }

  @Override
  public Record set(String fieldName, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getAsString(int index) {
    return String.valueOf(vector.getEntry(index));
  }

  @Override
  public double getAsDouble(int index) {
    return vector.getEntry(index);
  }

}
