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

import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DistributeRecommendWorkFn extends OryxReduceDoFn<Long, Pair<float[], LongSet>,
    Pair<Integer, Pair<Long, Pair<float[], LongSet>>>> {

  private static final Logger log = LoggerFactory.getLogger(DistributeRecommendWorkFn.class);

  private int numReducers;

  @Override
  public void initialize() {
    super.initialize();
    numReducers = getContext().getNumReduceTasks();
    log.info("Distributing data to {} reducers", numReducers);
  }

  @Override
  public void process(Pair<Long, Pair<float[], LongSet>> input,
                      Emitter<Pair<Integer, Pair<Long, Pair<float[], LongSet>>>> emitter) {
    for (int i = 0; i < numReducers; i++) {
      emitter.emit(Pair.of(i, input));
    }
  }
}
