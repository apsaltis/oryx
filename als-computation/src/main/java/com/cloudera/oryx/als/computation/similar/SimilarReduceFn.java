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

package com.cloudera.oryx.als.computation.similar;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.common.TopN;
import com.cloudera.oryx.als.computation.IDMappingState;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import com.google.common.base.Preconditions;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.io.IOException;

public final class SimilarReduceFn extends OryxReduceDoFn<Long, Iterable<NumericIDValue>, String> {

  private int numSimilar;
  private IDMappingState idMapping;

  @Override
  public void initialize() {
    super.initialize();
    numSimilar = ConfigUtils.getDefaultConfig().getInt("model.item-similarity.how-many");
    Preconditions.checkArgument(numSimilar > 0, "# similar must be positive: %s", numSimilar);
    try {
      idMapping = new IDMappingState(getConfiguration());
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  @Override
  public void process(Pair<Long, Iterable<NumericIDValue>> input, Emitter<String> emitter) {
    StringLongMapping mapping = idMapping.getIDMapping();
    Iterable<NumericIDValue> mostSimilar = TopN.selectTopN(input.second().iterator(), numSimilar);
    String item1ID = mapping.toString(input.first());
    for (NumericIDValue similar : mostSimilar) {
      emitter.emit(DelimitedDataUtils.encode(item1ID,
                                             mapping.toString(similar.getID()),
                                             Float.toString(similar.getValue())));
    }
  }

}
