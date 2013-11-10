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

package com.cloudera.oryx.als.computation.recommend;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.common.TopN;
import com.cloudera.oryx.als.computation.ComputationDataUtils;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import com.google.common.base.Preconditions;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public final class RecommendReduceFn extends OryxReduceDoFn<Integer,
    Iterable<Pair<Long, Pair<float[], LongSet>>>,
    Pair<Long,NumericIDValue>> {

  private int numRecs;
  private LongObjectMap<float[]> partialY;

  @Override
  public void initialize() {
    super.initialize();
    Configuration conf = getConfiguration();
    String yKey = conf.get(RecommendStep.Y_KEY_KEY);
    try {
      partialY = ComputationDataUtils.loadPartialY(getPartition(), getNumPartitions(), yKey, conf);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
    numRecs = ConfigUtils.getDefaultConfig().getInt("model.recommend.how-many");
    Preconditions.checkArgument(numRecs > 0, "# recs must be positive: %s", numRecs);
  }

  @Override
  public void process(Pair<Integer, Iterable<Pair<Long, Pair<float[], LongSet>>>> input,
                      Emitter<Pair<Long, NumericIDValue>> emitter) {
    Preconditions.checkState(input.first() == getPartition(),
        "Key must match partition: %s != %s", input.first(), getPartition());
    for (Pair<Long, Pair<float[], LongSet>> value : input.second()) {
      long userID = value.first();
      float[] userFeatures = value.second().first();
      LongSet knownItemIDs = value.second().second();
      Iterable<NumericIDValue> recs = TopN.selectTopN(
          new RecommendIterator(userFeatures, partialY.entrySet().iterator(), knownItemIDs),
          numRecs);
      for (NumericIDValue rec : recs) {
        emitter.emit(Pair.of(userID, rec));
      }
    }
  }
}
