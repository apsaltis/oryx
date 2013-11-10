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

package com.cloudera.oryx.computation.common.records.csv;

import java.util.Arrays;
import java.util.List;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.computation.common.records.BasicSpec;
import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.Spec;
import com.google.common.collect.Lists;

public final class CSVRecord implements Record {

  private final List<String> values;
  
  public CSVRecord(String... values) {
    this(Arrays.asList(values));
  }
  
  public CSVRecord(List<String> values) {
    this.values = values;
  }
  
  @Override
  public Spec getSpec() {
    return new BasicSpec(DataType.STRING, values.size());
  }

  @Override
  public int size() {
    return values.size();
  }
  
  @Override
  public Record copy(boolean deep) {
    if (deep) {
      List<String> v = Lists.newArrayList(values);
      return new CSVRecord(v);
    } else {
      List<String> v = Arrays.asList(new String[values.size()]);
      return new CSVRecord(v);
    }
  }
  
  @Override
  public Object get(int index) {
    return values.get(index);
  }
  
  @Override
  public Boolean getBoolean(String fieldName) {
    return Boolean.valueOf(getString(fieldName));
  }

  @Override
  public Double getDouble(String fieldName) {
    return Double.valueOf(getString(fieldName));
  }

  @Override
  public Integer getInteger(String fieldName) {
    return Integer.valueOf(getString(fieldName));
  }

  @Override
  public Long getLong(String fieldName) {
    return Long.valueOf(getString(fieldName));
  }

  @Override
  public String getString(String fieldName) {
    return values.get(getSpec().getField(fieldName).position());
  }

  @Override
  public Record set(String fieldName, Object value) {
    values.set(getSpec().getField(fieldName).position(), value.toString());
    return this;
  }

  @Override
  public Boolean getBoolean(int index) {
    return Boolean.valueOf(values.get(index));
  }

  @Override
  public Double getDouble(int index) {
    return Double.valueOf(values.get(index));
  }

  @Override
  public Integer getInteger(int index) {
    return Integer.valueOf(values.get(index));
  }

  @Override
  public Long getLong(int index) {
    return Long.valueOf(values.get(index));
  }

  @Override
  public String getString(int index) {
    return values.get(index);
  }

  @Override
  public Record set(int index, Object value) {
    values.set(index, value.toString());
    return this;
  }

  @Override
  public String getAsString(int index) {
    return values.get(index);
  }

  @Override
  public double getAsDouble(int index) {
    try {
      return Double.valueOf(values.get(index));
    } catch (NumberFormatException ignored) {
      return Double.NaN;
    }
  }
  
  @Override
  public String toString() {
    return DelimitedDataUtils.encode(values);
  }
}
