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

package com.cloudera.oryx.kmeans.computation.evaluate;

import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.cloudera.oryx.kmeans.computation.AvroUtils;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.util.List;

public final class WeightVectorsFn extends OryxDoFn<ClosestSketchVectorData, Pair<Integer, WeightedRealVector>> {

  private KSketchIndex index;
  private final String indexKey;

  public WeightVectorsFn(String indexKey) {
    this.indexKey = indexKey;
  }

  public WeightVectorsFn(KSketchIndex index) {
    this.index = index;
    this.indexKey = null;
  }

  @Override
  public void initialize() {
    super.initialize();
    if (index == null) {
      try {
        index = AvroUtils.readSerialized(indexKey, getConfiguration());
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  @Override
  public void process(ClosestSketchVectorData input, Emitter<Pair<Integer, WeightedRealVector>> emitter) {
    List<List<WeightedRealVector>> data = index.getWeightedVectors(input);
    for (int foldId = 0; foldId < data.size(); foldId++) {
      for (WeightedRealVector wrv : data.get(foldId)) {
        emitter.emit(Pair.of(foldId, wrv));
      }
    }
  }
}
