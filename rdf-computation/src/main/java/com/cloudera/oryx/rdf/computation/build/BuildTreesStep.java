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

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;

import java.io.IOException;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.rdf.computation.RDFJobStep;

/**
 * @author Sean Owen
 */
public final class BuildTreesStep extends RDFJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {

    JobStepConfig config = getConfig();
    String instanceGenerationPrefix =
        Namespaces.getInstanceGenerationPrefix(config.getInstanceDir(), config.getGenerationID());
    String outputPathKey = instanceGenerationPrefix + "trees/";
    if (!validOutputPath(outputPathKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(DistributeExampleFn.class);
    p.read(textInput(instanceGenerationPrefix + "inbound/"))
        .parallelDo("distributeData",
                    new DistributeExampleFn(),
                    Avros.tableOf(Avros.ints(), Avros.strings()))
        .groupByKey(groupingOptions())
        .parallelDo("buildTrees", new BuildTreeFn(), Avros.strings())
        .write(compressedTextOutput(p.getConfiguration(), outputPathKey));
    return p;
  }

  @Override
  protected boolean isHighMemoryStep() {
    return true;
  }

  public static void main(String[] args) throws Exception {
    run(new BuildTreesStep(), args);
  }

}
