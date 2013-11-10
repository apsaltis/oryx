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

package com.cloudera.oryx.als.computation.merge;

import com.google.common.base.Preconditions;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;

import java.io.IOException;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.als.computation.types.ALSTypes;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.JobStepConfig;

/**
 * @author Sean Owen
 */
public final class MergeIDMappingStep extends ALSJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {

    JobStepConfig jobConfig = getConfig();

    String instanceDir = jobConfig.getInstanceDir();
    long generationID = jobConfig.getGenerationID();
    long lastGenerationID = jobConfig.getLastGenerationID();

    String outputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "idMapping/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(MergeNewOldValuesFn.class);

    String inboundKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "inbound/";

    PTable<Long, String> parsed = p.read(textInput(inboundKey))
        .parallelDo("inboundParseForMapping", new MappingParseFn(),
            Avros.tableOf(ALSTypes.LONGS, Avros.strings()));

    if (lastGenerationID >= 0) {
      String idMappingPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "idMapping/";
      Preconditions.checkState(Store.get().exists(idMappingPrefix, false), "Input path does not exist: %s", idMappingPrefix);
      PTable<Long,String> joinBefore = p.read(textInput(idMappingPrefix))
          .parallelDo("lastGeneration", new ExistingMappingsMapFn(),
                      Avros.tableOf(ALSTypes.LONGS, Avros.strings()));
      parsed = parsed.union(joinBefore);
    }

    parsed.groupByKey(groupingOptions())
        .parallelDo("mergeNewOldMappings", new CombineMappingsFn(), Avros.strings())
        .write(compressedTextOutput(p.getConfiguration(), outputKey));
    return p;
  }

  public static void main(String[] args) throws Exception {
    run(new MergeIDMappingStep(), args);
  }

}
