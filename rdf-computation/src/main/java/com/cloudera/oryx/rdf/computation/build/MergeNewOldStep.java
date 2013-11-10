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

import com.google.common.base.Preconditions;
import org.apache.crunch.PCollection;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;

import java.io.IOException;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.rdf.computation.RDFJobStep;

/**
 * @author Sean Owen
 */
public final class MergeNewOldStep extends RDFJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {

    JobStepConfig jobConfig = getConfig();

    String instanceDir = jobConfig.getInstanceDir();
    long generationID = jobConfig.getGenerationID();
    long lastGenerationID = jobConfig.getLastGenerationID();

    String outputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "input/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(IdentityFn.class);

    String inboundKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "inbound/";

    PCollection<String> inbound = p.read(textInput(inboundKey));

    if (lastGenerationID >= 0) {
      String inputPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "input/";
      Preconditions.checkState(Store.get().exists(inputPrefix, false), "Input path does not exist: %s", inputPrefix);
      PCollection<String> lastInput = p.read(textInput(inputPrefix));
      inbound = inbound.union(lastInput);
    }

    inbound.write(compressedTextOutput(p.getConfiguration(), outputKey));
    return p;
  }

  public static void main(String[] args) throws Exception {
    run(new MergeNewOldStep(), args);
  }

}
