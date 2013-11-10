/**
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
package com.cloudera.oryx.kmeans.computation.covariance;

import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.google.common.collect.Maps;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;

import java.util.Map;

final class CoMomentKeyFn<K> extends OryxDoFn<Pair<K, Pair<RealVector, Double>>, Pair<Pair<K, Index>, CoMoment>> {
  private final Map<K, CoMomentTracker> trackers = Maps.newHashMap();
  private final PType<K> keyType;

  CoMomentKeyFn(PType<K> keyType) {
    this.keyType = keyType;
  }

  @Override
  public void initialize() {
    super.initialize();
    trackers.clear();
    keyType.initialize(getConfiguration());
  }

  @Override
  public void process(Pair<K, Pair<RealVector, Double>> input, Emitter<Pair<Pair<K, Index>, CoMoment>> emitter) {
    CoMomentTracker tracker = trackers.get(input.first());
    if (tracker == null) {
      tracker = new CoMomentTracker();
      trackers.put(keyType.getDetachedValue(input.first()), tracker);
    }
    tracker.update(input.second().first());
  }

  @Override
  public void cleanup(Emitter<Pair<Pair<K, Index>, CoMoment>> emitter) {
    for (Map.Entry<K, CoMomentTracker> e : trackers.entrySet()) {
      for (Map.Entry<Index, CoMoment> f : e.getValue().entrySet()) {
        emitter.emit(Pair.of(Pair.of(e.getKey(), f.getKey()), f.getValue()));
      }
    }
  }
}
