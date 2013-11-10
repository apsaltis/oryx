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

package com.cloudera.oryx.als.computation.known;

import java.io.IOException;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.als.computation.types.ALSTypes;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.computation.common.JobStepConfig;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;

/**
 * @author Sean Owen
 */
public final class CollectKnownItemsStep extends ALSJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {

    JobStepConfig config = getConfig();
    String instanceDir = config.getInstanceDir();
    long generationID = config.getGenerationID();

    String outputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "knownItems/";

    if (!validOutputPath(outputKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(CollectKnownItemsFn.class);
    // Really should read in and exclude tag IDs but doesn't really hurt much
    p.read(input(Namespaces.getTempPrefix(instanceDir, generationID) + "userVectors/", ALSTypes.SPARSE_ROW_MATRIX))
        .parallelDo("collectKnownItems", new CollectKnownItemsFn(), Avros.strings())
        .write(compressedTextOutput(p.getConfiguration(), outputKey));
    return p;
  }

  public static void main(String[] args) throws Exception {
    run(new CollectKnownItemsStep(), args);
  }

}
