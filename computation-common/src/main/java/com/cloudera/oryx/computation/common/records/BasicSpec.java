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

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public final class BasicSpec implements Spec {

  private final DataType dataType;
  private final int size;
  private final List<String> fieldNames;
  
  public BasicSpec(DataType dataType) {
    this(dataType, 0);
  }
  
  public BasicSpec(DataType dataType, int size) {
    this.dataType = Preconditions.checkNotNull(dataType);
    this.size = size;
    if (size > 0) {
      String[] n = new String[size];
      for (int i = 0; i < size; i++) {
        n[i] = "c" + i;
      }
      this.fieldNames = Arrays.asList(n);
    } else {
      this.fieldNames = ImmutableList.of();
    }
  }
  
  public BasicSpec(DataType dataType, List<String> fieldNames) {
    this.dataType = Preconditions.checkNotNull(dataType);
    this.size = fieldNames.size();
    this.fieldNames = fieldNames;
  }
  
  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public List<String> getFieldNames() {
    return fieldNames;
  }

  @Override
  public FieldSpec getField(final int index) {
    if (index < 0 || index >= size) {
      return null;
    }
    
    return new FieldSpec() {
      @Override
      public String name() {
        return fieldNames.get(index);
      }

      @Override
      public int position() {
        return index;
      }

      @Override
      public Spec spec() {
        return new BasicSpec(dataType, 0);
      }
    };
  }

  @Override
  public FieldSpec getField(final String fieldName) {
    if (!fieldNames.contains(fieldName)) {
      return null;
    }
    
    return new FieldSpec() {
      @Override
      public String name() {
        return fieldName;
      }

      @Override
      public int position() {
        return fieldNames.indexOf(fieldName);
      }

      @Override
      public Spec spec() {
        return new BasicSpec(dataType, 0);
      }
    };
  }
}
