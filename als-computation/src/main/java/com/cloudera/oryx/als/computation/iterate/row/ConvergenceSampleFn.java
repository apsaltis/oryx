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

package com.cloudera.oryx.als.computation.iterate.row;

import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.math.SimpleVectorMath;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import com.google.common.base.Preconditions;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConvergenceSampleFn extends OryxReduceDoFn<Long, float[], String> {

  private static final Logger log = LoggerFactory.getLogger(ConvergenceSampleFn.class);

  private final YState yState;
  private int convergenceSamplingModulus;

  public ConvergenceSampleFn(YState yState) {
    this.yState = yState;
  }

  @Override
  public void initialize() {
    super.initialize();
    convergenceSamplingModulus = getConfiguration().getInt(RowStep.CONVERGENCE_SAMPLING_MODULUS_KEY, -1);
    if (convergenceSamplingModulus >= 0) {
      Preconditions.checkArgument(convergenceSamplingModulus >= 0,
          "Not specified: %s",
          RowStep.CONVERGENCE_SAMPLING_MODULUS_KEY);
      log.info("Sampling for convergence where user/item ID == 0 % {}", convergenceSamplingModulus);
    }
    yState.initialize(getContext(), getPartition(), getNumPartitions());
  }

  @Override
  public void process(Pair<Long, float[]> input, Emitter<String> emitter) {
    if (input.first() % convergenceSamplingModulus == 0) {
      String userID = input.first().toString();
      float[] xu = input.second();
      for (LongObjectMap.MapEntry<float[]> entry : yState.getY().entrySet()) {
        long itemID = entry.getKey();
        if (itemID % convergenceSamplingModulus == 0) {
          float estimate = (float) SimpleVectorMath.dot(xu, entry.getValue());
          emitter.emit(DelimitedDataUtils.encode(userID, itemID, estimate));
        }
      }
    }
  }
}
