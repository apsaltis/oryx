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

import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.FieldSpec;
import com.cloudera.oryx.computation.common.records.Spec;

public final class CSVFieldSpec implements FieldSpec {

  private final String name;
  private final int pos;
  private final DataType dataType;
  
  public CSVFieldSpec(String name, int pos, DataType dataType) {
    this.name = name;
    this.pos = pos;
    this.dataType = dataType;
  }
  
  @Override
  public String name() {
    return name;
  }

  @Override
  public int position() {
    return pos;
  }

  @Override
  public Spec spec() {
    return new CSVSpec(dataType);
  }
}
