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

import java.io.IOException;

/**
 * Represents a "flat" (i.e., no nested records or collection types) collection
 * of named and typed fields.
 */
public interface Record {
  /**
   * Returns the {@code RecordSpec} for this Record instance, which describes the
   * names and types of its fields.
   */
  Spec getSpec();

  /**
   * Returns the number of fields in this record.
   */
  int size();
  
  /**
   * Creates a copy of this {@code Record} instance, either a shallow copy that has
   * the same {@code Spec} but not the same data, or a deep copy that contains copies
   * of all of the data stored in the fields of this instance.
   * 
   * @param deep Whether or not to perform a deep copy of the fields of this instance
   * @return A new {@code Record} with the same {@code Spec} as this instance
   */
  Record copy(boolean deep) throws IOException;

  /**
   * A generic getter for when the client is indifferent to the return type
   * of the field. The object returned is guaranteed to be of the type specified
   * by the {@code FieldSpec} for the given index.
   * 
   * @param index The index of the field to return
   * @return An object, or some sort of unpleasant exception if the index is out of bounds
   */
  Object get(int index);
  
  /**
   * Returns the value of the field at the given index coerced to a string via the toString
   * method on the object.
   * 
   * @param index The index of the field to return
   */
  String getAsString(int index);

  /**
   * Returns the value of the field at the given index coerced to be a double. If the
   * coercion is not successful, Double.NaN is returned instead.
   * 
   * @param index The index of the field to return
   */
  double getAsDouble(int index);

  /**
   * Convenience method for getting the Boolean value at the given index.
   * 
   * @param index The index of the field to return
   * @return A Boolean representation of the value at the given index
   */
  Boolean getBoolean(int index) throws IOException;

  /**
   * Convenience method for getting the Boolean value of a named field.
   * 
   * @param fieldName The name of the field to return
   * @return A Boolean representation of the value for the field
   */
  Boolean getBoolean(String fieldName) throws IOException;

  /**
   * Convenience method for getting the Double value at the given index.
   * 
   * @param index The index of the field to return
   * @return A Double representation of the value at the given index
   */
  Double getDouble(int index) throws IOException;
  
  /**
   * Convenience method for getting the Double value of a named field.
   * 
   * @param fieldName The name of the field to return
   * @return A Double representation of the value for the field
   */
  Double getDouble(String fieldName) throws IOException;
  
  /**
   * Convenience method for getting the Integer value at the given index.
   * 
   * @param index The index of the field to return
   * @return An Integer representation of the value at the given index
   */
  Integer getInteger(int index) throws IOException;

  /**
   * Convenience method for getting the Integer value of a named field.
   * 
   * @param fieldName The name of the field to return
   * @return An Integer representation of the value for the field
   */
  Integer getInteger(String fieldName) throws IOException;

  /**
   * Convenience method for getting the Long value at the given index.
   * 
   * @param index The index of the field to return
   * @return A Long representation of the value at the given index
   */
  Long getLong(int index) throws IOException;

  /**
   * Convenience method for getting the Long value of a named field.
   * 
   * @param fieldName The name of the field to return
   * @return A Long representation of the value for the field
   */
  Long getLong(String fieldName) throws IOException;

  /**
   * Convenience method for getting the String value at the given index.
   * 
   * @param index The index of the field to return
   * @return A String representation of the value at the given index
   */
  String getString(int index) throws IOException;
  
  /**
   * Convenience method for getting the String value of a named field.
   * 
   * @param fieldName The name of the field to return
   * @return A String representation of the value for the field
   */
  String getString(String fieldName) throws IOException;
  
  /**
   * Sets the value at the given index in this {@code Record} instance.
   * 
   * @param index The index to set
   * @param value The value to set the field to
   * @return This {@code Record} instance, for method chaining
   */
  Record set(int index, Object value);
  
  /**
   * Sets the value at the given name in this {@code Record} instance.
   * 
   * @param fieldName The name of the field to set
   * @param value The value to set the field to
   * @return This {@code Record} instance, for method chaining
   */
  Record set(String fieldName, Object value) throws IOException;
}
