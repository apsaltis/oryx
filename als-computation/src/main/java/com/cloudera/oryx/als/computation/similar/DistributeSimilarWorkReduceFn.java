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
import com.cloudera.oryx.als.common.TopN;
import com.cloudera.oryx.als.computation.ComputationDataUtils;
import com.cloudera.oryx.als.computation.types.MatrixRow;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import com.google.common.base.Preconditions;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public final class DistributeSimilarWorkReduceFn extends
    OryxReduceDoFn<Integer, Iterable<MatrixRow>, Pair<Long, NumericIDValue>> {

  private LongObjectMap<float[]> partialY;
  private int numSimilar;

  @Override
  public void initialize() {
    super.initialize();
    Configuration conf = getConfiguration();
    String yKey = conf.get(DistributeSimilarWorkStep.Y_KEY_KEY);
    try {
      partialY = ComputationDataUtils.loadPartialY(getPartition(), getNumPartitions(), yKey, conf);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
    numSimilar = ConfigUtils.getDefaultConfig().getInt("model.item-similarity.how-many");
    Preconditions.checkArgument(numSimilar > 0, "# similar must be positive: %s", numSimilar);
  }

  @Override
  public void process(Pair<Integer, Iterable<MatrixRow>> input, Emitter<Pair<Long, NumericIDValue>> emitter) {
    Preconditions.checkState(input.first() == getPartition(),
        "Key must match partition: %s != %s", input.first(), getPartition());
    for (MatrixRow value : input.second()) {
      long itemID = value.getRowId();
      float[] itemFeatures = value.getValues();
      Iterable<NumericIDValue> mostSimilar = TopN.selectTopN(
          new MostSimilarItemIterator(partialY.entrySet().iterator(), itemID, itemFeatures), numSimilar);
      for (NumericIDValue similar : mostSimilar) {
        emitter.emit(Pair.of(itemID, similar));
      }
    }
  }
}
