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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Utility functions for working with header files and {@code Spec} data.
 */
public final class Specs {

  public static boolean isNumeric(Spec spec, String fieldId) {
    FieldSpec fs = spec.getField(getFieldId(spec, fieldId));
    return fs.spec().getDataType().isNumeric();
  }
  
  public static Integer getFieldId(Spec spec, String value) {
    List<Integer> fieldIds = getFieldIds(spec, ImmutableList.of(value));
    if (fieldIds.isEmpty()) {
      throw new IllegalArgumentException("Could not find field " + value + " in spec");
    }
    return fieldIds.get(0);
  }

  public static List<Integer> getFieldIds(Spec spec, List<String> values) {
    if (values.isEmpty()) {
      return ImmutableList.of();
    }
    List<Integer> fieldIds = Lists.newArrayListWithExpectedSize(values.size());
    if (spec == null || spec.getField(values.get(0)) == null) {
      for (String value : values) {
        try {
          fieldIds.add(Integer.valueOf(value));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Did not recognize column ID: " + value);
        }
      }
    } else {
      for (String value : values) {
        FieldSpec f = spec.getField(value);
        if (f != null) {
          fieldIds.add(f.position());
        }
      }
    }
    return fieldIds;
  }
  
  private Specs() {
  }
}
