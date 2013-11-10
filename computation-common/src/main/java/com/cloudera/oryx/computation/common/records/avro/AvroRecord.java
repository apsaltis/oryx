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

package com.cloudera.oryx.computation.common.records.avro;

import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.Spec;
import org.apache.avro.generic.GenericData;

public final class AvroRecord implements Record {

  private final GenericData.Record impl;
  
  public AvroRecord(GenericData.Record impl) {
    this.impl = impl;
  }
  
  public GenericData.Record getImpl() {
    return impl;
  }
  
  @Override
  public int size() {
    return impl.getSchema().getFields().size();
  }
  
  @Override
  public Record copy(boolean deep) {
    if (deep) {
      return new AvroRecord(new GenericData.Record(impl, true));
    } else {
      return new AvroRecord(new GenericData.Record(impl.getSchema()));
    }
  }
  
  @Override
  public Spec getSpec() {
    return new AvroSpec(impl.getSchema());
  }

  @Override
  public Object get(int index) {
    return impl.get(index);
  }
  
  @Override
  public Boolean getBoolean(int index) {
    return (Boolean) impl.get(index);
  }
  
  @Override
  public Boolean getBoolean(String fieldName) {
    return (Boolean) impl.get(fieldName);
  }

  @Override
  public Double getDouble(int index) {
    return (Double) impl.get(index);
  }
  
  @Override
  public Double getDouble(String fieldName) {
    return (Double) impl.get(fieldName);
  }

  @Override
  public Integer getInteger(int index) {
    return (Integer) impl.get(index);
  }
  
  @Override
  public Integer getInteger(String fieldName) {
    return (Integer) impl.get(fieldName);
  }

  @Override
  public Long getLong(int index) {
    return (Long) impl.get(index);
  }
  
  @Override
  public Long getLong(String fieldName) {
    return (Long) impl.get(fieldName);
  }

  @Override
  public String getString(int index) {
    return (String) impl.get(index);
  }
  
  @Override
  public String getString(String fieldName) {
    return (String) impl.get(fieldName);
  }

  @Override
  public Record set(int index, Object value) {
    impl.put(index, value);
    return this;
  }
  
  @Override
  public Record set(String fieldName, Object value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public String getAsString(int index) {
    return impl.get(index).toString();
  }

  @Override
  public double getAsDouble(int index) {
    return ((Number) impl.get(index)).doubleValue();
  }
  
  @Override
  public String toString() {
    return impl.toString();
  }
}
