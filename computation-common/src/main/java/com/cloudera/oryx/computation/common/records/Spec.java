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

import java.io.Serializable;
import java.util.List;


/**
 * Contains type information about a {@code Record} instance or about the
 * specific fields contained in a {@code Record}.
 */
public interface Spec extends Serializable {
  /**
   * Returns The {@code DataType} for this record or field.
   */
  DataType getDataType();
  
  /**
   * Returns the number of fields in a {@code Record} instance.
   */
  int size();
  
  /**
   * Returns the names of the fields in a {@code Record} instance.
   */
  List<String> getFieldNames();

  /**
   * Returns the {@code FieldSpec} associated with a field index.
   * @param index The index of the field
   * @return The {@code FieldSpec} for the given index
   */
  FieldSpec getField(int index);
  
  /**
   * Returns the {@code FieldSpec} associated with a field name.
   * @param fieldName The name of the field
   * @return The {@code FieldSpec} for the given field name
   */
  FieldSpec getField(String fieldName);  
}
