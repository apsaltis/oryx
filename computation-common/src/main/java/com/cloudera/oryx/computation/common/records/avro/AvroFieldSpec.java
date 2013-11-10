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

import com.cloudera.oryx.computation.common.records.Spec;
import org.apache.avro.Schema;

import com.cloudera.oryx.computation.common.records.FieldSpec;

public final class AvroFieldSpec implements FieldSpec {

  private final String name;
  private final int position;
  private final String schemaJson;
  private transient Schema schema;
  
  public AvroFieldSpec(Schema.Field field) {
    this.name = field.name();
    this.position = field.pos();
    this.schema = field.schema();
    this.schemaJson = schema.toString();
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
    if (schema == null) {
      schema = (new Schema.Parser()).parse(schemaJson);
    }
    return new AvroSpec(schema);
  }

}
