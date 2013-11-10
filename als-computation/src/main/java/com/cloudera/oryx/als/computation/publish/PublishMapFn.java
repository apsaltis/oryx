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

package com.cloudera.oryx.als.computation.publish;

import com.cloudera.oryx.als.computation.types.MatrixRow;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.computation.common.fn.OryxMapFn;

public final class PublishMapFn extends OryxMapFn<MatrixRow, String> {
  @Override
  public String map(MatrixRow input) {
    return String.valueOf(input.getRowId()) + '\t' + vectorToString(input.getValues());
  }

  private static String vectorToString(float[] vector) {
    String[] stringFeatures = new String[vector.length];
    for (int i = 0; i < stringFeatures.length; i++) {
      stringFeatures[i] = Float.toString(vector[i]);
    }
    return DelimitedDataUtils.encode(stringFeatures);
  }

}
