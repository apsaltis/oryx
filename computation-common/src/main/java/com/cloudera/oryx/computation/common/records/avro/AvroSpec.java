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

import java.util.List;

import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.Spec;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import com.cloudera.oryx.computation.common.records.FieldSpec;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public final class AvroSpec implements Spec {

  private final String schemaJson;
  private final DataType dataType;
  private transient Schema schema;
  
  private static DataType getDataType(Schema schema) {
    Schema.Type st = schema.getType();
    Preconditions.checkNotNull(st);
    switch (st) {
    case RECORD:
      return DataType.RECORD;
    case INT:
      return DataType.INT;
    case BOOLEAN:
      return DataType.BOOLEAN;
    case FLOAT:
    case DOUBLE:
      return DataType.DOUBLE;
    case STRING:
      return DataType.STRING;
    case LONG:
      return DataType.LONG;
      default:
        throw new IllegalStateException("Cannot support schema type = " + st);
    }
  }
  
  public AvroSpec(Schema schema) {
    this.schema = schema;
    this.schemaJson = schema.toString();
    this.dataType = getDataType(schema);
  }
  
  public Schema getSchema() {
    if (schema == null) {
      schema = (new Schema.Parser()).parse(schemaJson);
    }
    return schema;
  }

  private List<Field> getFields() {
    return getSchema().getFields();
  }
  
  @Override
  public int size() {
    return getFields().size();
  }

  @Override
  public List<String> getFieldNames() {
    return Lists.transform(getFields(), new Function<Field, String>() {
      @Override
      public String apply(Field input) {
        return input.name();
      }
    });
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public FieldSpec getField(String fieldName) {
    return new AvroFieldSpec(getSchema().getField(fieldName));
  }

  @Override
  public FieldSpec getField(int index) {
    return new AvroFieldSpec(getFields().get(index));
  }
}
