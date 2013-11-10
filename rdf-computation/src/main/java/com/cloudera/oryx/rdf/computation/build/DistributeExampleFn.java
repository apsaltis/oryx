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

package com.cloudera.oryx.rdf.computation.build;

import com.typesafe.config.Config;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.util.FastMath;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;

public final class DistributeExampleFn extends OryxDoFn<String,Pair<Integer,String>> {

  private static final Logger log = LoggerFactory.getLogger(DistributeExampleFn.class);

  private int numReducers;
  private int reducersPerDatum;
  private final RandomDataGenerator random = new RandomDataGenerator(RandomManager.getRandom());

  @Override
  public void initialize() {
    super.initialize();
    numReducers = getContext().getNumReduceTasks();
    log.info("{} reducers", numReducers);

    Config config = ConfigUtils.getDefaultConfig();
    int numTrees = config.getInt("model.num-trees");
    // Bump this up to as least 2x reducers
    numTrees = FastMath.max(numTrees, 2 * numReducers);
    // Make it a multiple of # reducers
    while ((numTrees % numReducers) != 0) {
      numTrees++;
    }

    int foldsPerReducer = numTrees / numReducers;
    log.info("{} folds per reducer", foldsPerReducer);

    double sampleRate = config.getDouble("model.sample-rate");
    reducersPerDatum = 1;
    while ((double) (reducersPerDatum * foldsPerReducer - 1) / numTrees < sampleRate) {
      reducersPerDatum++;
    }

    log.info("{} reducers per datum", reducersPerDatum);
  }

  @Override
  public void process(String input, Emitter<Pair<Integer,String>> emitter) {
    // Similar to:
    // http://blog.cloudera.com/blog/2013/02/how-to-resample-from-a-large-data-set-in-parallel-with-r-on-hadoop/
    // Here, KM > N; let K = S*(M/N). We don't know N. We know S = reducersPerDatum and we have M = numReducers.
    // Each data point can be sent to S reducers chosen uniformly at random. Expected # of data points at each
    // reducer has a binomial distribution with mean K, as desired. For large N this is virtually the same distribution
    // as in the link above, which is Poisson with mean K.
    for (int reducer : random.nextPermutation(numReducers, reducersPerDatum)) {
      emitter.emit(Pair.of(reducer, input));
    }
  }

}
