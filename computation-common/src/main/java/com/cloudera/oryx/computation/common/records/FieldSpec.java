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


/**
 * Stores name, type, and position for a field in a {@code Record}.
 */
public interface FieldSpec extends Serializable {
  /**
   * Returns the name of this field.
   */
  String name();
  
  /**
   * Returns the zero-indexed position of this field in the {@code Record}.
   */
  int position();

  /**
   * Returns the {@code Spec} that contains type information for this field.
   */
  Spec spec();
}
