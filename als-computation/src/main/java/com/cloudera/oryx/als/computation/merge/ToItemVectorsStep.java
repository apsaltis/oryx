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

package com.cloudera.oryx.als.computation.merge;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.computation.types.ALSTypes;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;

/**
 * @author Sean Owen
 */
public final class ToItemVectorsStep extends AbstractToVectorsStep {

  @Override
  PTable<Long, NumericIDValue> getMatrix(MRPipeline p, String inputKey) {
    return p.read(input(inputKey, ALSTypes.VALUE_MATRIX))
        .parallelDo("transposeUserItem", new TransposeUserItemFn(),
            Avros.tableOf(ALSTypes.LONGS, ALSTypes.IDVALUE));
  }

  @Override
  String getSuffix() {
    return "itemVectors/";
  }

  public static void main(String[] args) throws Exception {
    run(new ToItemVectorsStep(), args);
  }
  
}
