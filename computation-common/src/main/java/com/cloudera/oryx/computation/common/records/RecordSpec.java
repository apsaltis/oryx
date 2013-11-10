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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.cloudera.oryx.common.io.DelimitedDataUtils;

/**
 * The common implementation of the {@code Spec} interface for {@code Record} instances.
 */
public final class RecordSpec implements Spec {

  private final List<FieldSpec> fields;
  
  RecordSpec(List<FieldSpec> fields) {
    this.fields = fields;
  }
  
  public Header toHeader() {
    Header.Builder hb = Header.builder();
    for (FieldSpec fs : fields) {
      if (fs.spec().getDataType().isNumeric()) {
        hb.addNumeric(fs.name());
      } else {
        hb.addCategorical(fs.name());
      }
    }
    return hb.build();
  }
  
  @Override
  public DataType getDataType() {
    return DataType.RECORD;
  }

  @Override
  public int size() {
    return fields.size();
  }

  @Override
  public List<String> getFieldNames() {
    return Lists.transform(fields, new Function<FieldSpec, String>() {
      @Override
      public String apply(FieldSpec input) {
        return input.name();
      }
    });
  }

  @Override
  public FieldSpec getField(int index) {
    return fields.get(index);
  }

  @Override
  public FieldSpec getField(String fieldName) {
    for (FieldSpec field : fields) {
      if (field.name().equals(fieldName)) {
        return field;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return DelimitedDataUtils.encode(fields);
  }
  
  public static Builder builder() {
    return new Builder();
  }
  
  public static Builder builder(Spec base) {
    return new Builder(base);
  }
  
  public static final class Builder {
    private final List<FieldSpec> fields = Lists.newArrayList();
    
    public Builder() { }

    public Builder(Spec base) {
      for (int i = 0; i < base.size(); i++) {
        FieldSpec f = base.getField(i);
        fields.add(new FieldSpecImpl(f.name(), i, f.spec()));
      }
    }
    
    public Builder add(String name, Spec spec) {
      fields.add(new FieldSpecImpl(name, fields.size(), spec));
      return this;
    }
    
    public Builder add(String name, DataType dataType) {
      return add(name, new BasicSpec(dataType));
    }

    public Builder addBoolean(String name) {
      return add(name, DataType.BOOLEAN);
    }
    
    public Builder addInt(String name) {
      return add(name, DataType.INT);
    }
    
    public Builder addLong(String name) {
      return add(name, DataType.LONG);
    }
    
    public Builder addDouble(String name) {
      return add(name, DataType.DOUBLE);
    }
    
    public Builder addString(String name) {
      return add(name, DataType.STRING);
    }
    
    public RecordSpec build() {
      return new RecordSpec(fields);
    }
  }
  
  private static final class FieldSpecImpl implements FieldSpec {
    private final String name;
    private final int position;
    private final Spec spec;
    
    private FieldSpecImpl(String name, int position, Spec spec) {
      this.name = name;
      this.position = position;
      this.spec = spec;
    }
    
    @Override
    public String name() {
      return name;
    }
    
    @Override
    public int position() {
      return position;
    }
    
    @Override
    public Spec spec() {
      return spec;
    }
  }
}
