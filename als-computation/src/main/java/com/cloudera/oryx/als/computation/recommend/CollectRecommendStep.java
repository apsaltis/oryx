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

import java.io.IOException;

import com.cloudera.oryx.als.computation.IDMappingState;
import com.cloudera.oryx.als.computation.types.ALSTypes;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.avro.Avros;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.common.servcomp.Namespaces;

/**
 * @author Sean Owen
 */
public final class CollectRecommendStep extends ALSJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {

    JobStepConfig config = getConfig();
    String instanceDir = config.getInstanceDir();
    long generationID = config.getGenerationID();

    String inputKey = Namespaces.getTempPrefix(instanceDir, generationID) + "partialRecommend/";
    String outputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "recommend/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(CollectRecommendFn.class);
    p.getConfiguration().set(IDMappingState.ID_MAPPING_KEY,
                             Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "idMapping/");
    PTables.asPTable(p.read(input(inputKey, ALSTypes.VALUE_MATRIX)))
        .groupByKey(groupingOptions())
        .parallelDo("collectRecommend", new CollectRecommendFn(), Avros.strings())
        .write(compressedTextOutput(p.getConfiguration(), outputKey));
    return p;
  }

  public static void main(String[] args) throws Exception {
    run(new CollectRecommendStep(), args);
  }

}
