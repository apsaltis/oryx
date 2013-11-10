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

package com.cloudera.oryx.kmeans.computation.cluster;

import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.cloudera.oryx.kmeans.computation.AvroUtils;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class UpdateIndexFn extends OryxDoFn<Pair<Integer, RealVector>, KSketchIndex> {

  private String indexKey;
  private KSketchIndex index;

  UpdateIndexFn(String indexKey) {
    this.indexKey = indexKey;
  }

  public UpdateIndexFn(KSketchIndex index) {
    this.index = index;
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
  public void process(Pair<Integer, RealVector> input, Emitter<KSketchIndex> emitter) {
    index.add(input.second(), input.first());
  }

  @Override
  public void cleanup(Emitter<KSketchIndex> emitter) {
    emitter.emit(index);
  }
}
