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

import java.io.IOException;

import com.cloudera.oryx.als.computation.types.ALSTypes;
import com.cloudera.oryx.als.computation.types.MatrixRow;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.als.computation.iterate.IterationState;
import com.cloudera.oryx.als.computation.iterate.IterationStep;
import com.cloudera.oryx.common.random.RandomUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;

/**
 * @author Sean Owen
 */
public final class RowStep extends IterationStep {

  private static final Logger log = LoggerFactory.getLogger(RowStep.class);

  public static final String Y_KEY_KEY = "Y_KEY";
  public static final String POPULAR_KEY = "POPULAR";
  public static final String CONVERGENCE_SAMPLING_MODULUS_KEY = "CONVERGENCE_SAMPLING_MODULUS";

  @Override
  protected MRPipeline createPipeline() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    boolean x = iterationState.isComputingX();
    int lastIteration = iterationState.getIteration() - 1;
    Store store = Store.get();

    JobStepConfig config = getConfig();
    String instanceDir = config.getInstanceDir();
    long generationID = config.getGenerationID();

    if (store.exists(Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "X/", false)) {
      // Actually, looks like whole computation of X/Y finished -- just proceed
      return null;
    }

    // Take the opportunity to clean out iteration before last, if computing X
    if (x) {
      String lastLastIterationKey =
          Namespaces.getIterationsPrefix(instanceDir, generationID) + (lastIteration - 1) + '/';
      if (store.exists(lastLastIterationKey, false)) {
        log.info("Deleting old iteration data from {}", lastLastIterationKey);
        store.recursiveDelete(lastLastIterationKey);
      }
    }

    String yKey;
    if (x) {
      yKey = Namespaces.getIterationsPrefix(instanceDir, generationID) + lastIteration + "/Y/";
    } else {
      yKey = iterationKey + "X/";
    }

    String xKey = iterationKey + (x ? "X/" : "Y/");
    String rKey = Namespaces.getTempPrefix(instanceDir, generationID) + (x ? "userVectors/" : "itemVectors/");

    if (!validOutputPath(xKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(RowReduceFn.class);
    Configuration conf = p.getConfiguration();
    conf.set(Y_KEY_KEY, yKey);

    String tempKey = Namespaces.getTempPrefix(instanceDir, generationID);
    String popularKey = tempKey + (x ? "popularItemsByUserPartition/" : "popularUsersByItemPartition/");
    conf.set(POPULAR_KEY, popularKey);

    YState yState = new YState(ALSTypes.DENSE_ROW_MATRIX); // Shared Y-Matrix state

    GroupingOptions opts = groupingOptions();
    PCollection<MatrixRow> matrix = PTables.asPTable(p.read(input(rKey, ALSTypes.SPARSE_ROW_MATRIX)))
        .groupByKey(opts)
        .parallelDo("rowReduce", new RowReduceFn(yState), ALSTypes.DENSE_ROW_MATRIX)
        .write(output(xKey));

    if (!x) {
      // Configure and perform convergence sampling
      int modulus = chooseConvergenceSamplingModulus(opts);
      conf.setInt(CONVERGENCE_SAMPLING_MODULUS_KEY, modulus);

      matrix
          .parallelDo("asPair", MatrixRow.AS_PAIR, Avros.tableOf(Avros.longs(), ALSTypes.FLOAT_ARRAY))
          .parallelDo("convergenceSample", new ConvergenceSampleFn(yState), Avros.strings())
          .write(compressedTextOutput(p.getConfiguration(), iterationKey + "Yconvergence"));
    }
    return p;
  }
  
  private static int chooseConvergenceSamplingModulus(GroupingOptions opts) {
    // Kind of arbitrary formula, determined empirically.
    int modulus = RandomUtils.nextTwinPrime(16 * opts.getNumReducers() * opts.getNumReducers());
    log.info("Using convergence sampling modulus {} to sample about {}% of all user-item pairs for convergence",
             modulus, 100.0f / modulus / modulus);
    return modulus;
  }

  @Override
  protected boolean isHighMemoryStep() {
    return true;
  }

  public static void main(String[] args) throws Exception {
    run(new RowStep(), args);
  }

}
