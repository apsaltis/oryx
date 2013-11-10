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

package com.cloudera.oryx.computation.common.records;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.cloudera.oryx.common.io.DelimitedDataUtils;

public final class Header {

  public enum Type {
    ID,
    IGNORED,
    NUMERIC,
    CATEGORICAL
  }

  private final Map<String, Type> data;
  
  public static final class Builder {
    
    private final Map<String, Type> data = Maps.newLinkedHashMap();
    
    public Builder addNumeric(String name) {
      data.put(name, Type.NUMERIC);
      return this;
    }
    
    public Builder addCategorical(String name) {
      data.put(name, Type.CATEGORICAL);
      return this;
    }
    
    public Builder addIgnored(String name) {
      data.put(name, Type.IGNORED);
      return this;
    }
    
    public Builder addIdentifier(String name) {
      data.put(name, Type.ID);
      return this;
    }
    
    public Header build() {
      return new Header(data);
    }
  }
  
  public static Builder builder() {
    return new Builder();
  }
  
  private Header(Map<String, Type> data) {
    this.data = data;
  }
  
  public Spec toSpec() {
    RecordSpec.Builder rsb = RecordSpec.builder();
    for (Map.Entry<String, Type> e : data.entrySet()) {
      switch (e.getValue()) {
      case NUMERIC:
        rsb.add(e.getKey(), DataType.DOUBLE);
        break;
      case CATEGORICAL:
      case ID:
      case IGNORED:
        rsb.add(e.getKey(), DataType.STRING);
        break;
      }
    }
    return rsb.build();
  }
  
  public List<Integer> getNumericColumns() {
    return getColumns(Type.NUMERIC);
  }
  
  public List<Integer> getCategoricalColumns() {
    return getColumns(Type.CATEGORICAL);
  }
  
  public List<Integer> getIgnoredColumns() {
    return getColumns(Type.IGNORED, Type.ID);
  }
  
  public Integer getIdColumn() {
    List<Integer> id = getColumns(Type.ID);
    if (id.isEmpty()) {
      return null;
    } else {
      return id.get(0);
    }
  }
  
  private List<Integer> getColumns(Type... targets) {
    List<Integer> ret = Lists.newArrayList();
    int index = 0;
    for (Type t : data.values())  {
      for (Type target : targets) {
        if (t == target) {
          ret.add(index);
          break;
        }
      }
      index++;
    }
    return ret;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Type> e : data.entrySet()) {
      sb.append(DelimitedDataUtils.encode(e.getKey(), e.getValue().toString().toLowerCase(Locale.ENGLISH)))
          .append('\n');
    }
    return sb.toString();
  }
}
