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

package com.cloudera.oryx.als.computation.initialy;

import java.io.IOException;

import com.cloudera.oryx.als.computation.types.ALSTypes;

import org.apache.crunch.PTable;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.avro.Avros;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;

/**
 * @author Sean Owen
 */
public final class InitialYStep extends ALSJobStep {

  private static final Logger log = LoggerFactory.getLogger(InitialYStep.class);

  @Override
  protected MRPipeline createPipeline() throws IOException {

    JobStepConfig alsConfig = getConfig();
    String instanceDir = alsConfig.getInstanceDir();
    long generationID = alsConfig.getGenerationID();
    String iterationsPrefix = Namespaces.getIterationsPrefix(instanceDir, generationID);
    Store store = Store.get();

    String initialYKey = iterationsPrefix + "0/Y/";
    if (store.exists(iterationsPrefix, false) &&
        (!store.exists(initialYKey, false) || store.exists(initialYKey + "_SUCCESS", true))) {
      // If iterations exist but iterations/0/Y was deleted, or it exists and clearly succeeded, skip
      log.info("Skipping because iterations started but iteration 0 is done or deleted");
      return null;
    }
    if (store.exists(Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "X/", false)) {
      // Actually, looks like whole computation of X/Y finished -- just proceed
      log.info("Skipping because factorization appears to have finished");
      return null;
    }

    if (!validOutputPath(initialYKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(FlagNewItemsFn.class);

    String newItemsPath = Namespaces.getTempPrefix(instanceDir, generationID) + "itemVectors/";
    PTable<Long, float[]> input = PTables.asPTable(p.read(input(newItemsPath, ALSTypes.SPARSE_ROW_MATRIX))
        .parallelDo("flagNewItems", new FlagNewItemsFn(), Avros.tableOf(Avros.longs(), ALSTypes.FLOAT_ARRAY)));

    // Optionally override by reading in last generation's Y as starting value
    long lastGenerationID = alsConfig.getLastGenerationID();
    if (lastGenerationID >= 0) {
      String yPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "Y/";
      if (store.exists(yPrefix, false)) {
        PTable<Long, float[]> last = PTables.asPTable(p.read(textInput(yPrefix))
            .parallelDo("copyOld", new CopyPreviousYFn(), Avros.tableOf(Avros.longs(), ALSTypes.FLOAT_ARRAY)));
        input = input.union(last);
      } else {
        log.warn("Previous generation exists, but no Y; this should only happen if the model was not generated " +
                 "due to insufficient rank");
      }
    }

    input.groupByKey(groupingOptions())
        .combineValues(new PreviousOrEmptyFeaturesAggregator())
        .parallelDo("initialYReduce", new InitialYReduceFn(), ALSTypes.DENSE_ROW_MATRIX)
        .write(output(initialYKey));
    return p;
  }

  public static void main(String[] args) throws Exception {
    run(new InitialYStep(), args);
  }

}
