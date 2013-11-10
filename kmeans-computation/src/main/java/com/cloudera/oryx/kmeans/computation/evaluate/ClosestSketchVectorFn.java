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
import com.cloudera.oryx.kmeans.common.Distance;
import com.cloudera.oryx.kmeans.computation.AvroUtils;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import com.cloudera.oryx.kmeans.computation.cluster.ClusterSettings;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class ClosestSketchVectorFn<V extends RealVector> extends OryxDoFn<Pair<Integer, V>, Pair<Integer, ClosestSketchVectorData>> {

  private KSketchIndex index;
  private final String indexKey;
  private final ClusterSettings settings;
  private ClosestSketchVectorData data;

  public ClosestSketchVectorFn(String indexKey, ClusterSettings settings) {
    this.indexKey = indexKey;
    this.settings = settings;
  }

  public ClosestSketchVectorFn(KSketchIndex index, ClusterSettings settings) {
    this.index = index;
    this.indexKey = null;
    this.settings = settings;
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
    data = new ClosestSketchVectorData(settings.getCrossFolds(), settings.getTotalPoints());
  }

  @Override
  public void process(Pair<Integer, V> in, Emitter<Pair<Integer, ClosestSketchVectorData>> emitter) {
    int foldId = in.first();
    Distance d = index.getDistance(in.second(), foldId, true);
    data.inc(foldId, d.getClosestCenterId());
  }

  @Override
  public void cleanup(Emitter<Pair<Integer, ClosestSketchVectorData>> emitter) {
    // Everything gets a key of 0 so I can perform a groupby later to agg all output
    emitter.emit(Pair.of(0, data));
  }
}
