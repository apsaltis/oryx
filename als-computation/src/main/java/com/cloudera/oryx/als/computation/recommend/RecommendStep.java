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

import com.cloudera.oryx.als.computation.types.ALSTypes;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.PTables;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.als.computation.iterate.IterationState;
import com.cloudera.oryx.als.computation.iterate.IterationStep;
import com.cloudera.oryx.common.servcomp.Namespaces;

/**
 * @author Sean Owen
 */
public final class RecommendStep extends IterationStep {

  public static final String Y_KEY_KEY = "Y_KEY";

  @Override
  protected MRPipeline createPipeline() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    JobStepConfig config = getConfig();
    String instanceDir = config.getInstanceDir();
    long generationID = config.getGenerationID();
    String tempPrefix = Namespaces.getTempPrefix(instanceDir, generationID);
    String outputKey = tempPrefix + "partialRecommend/";

    if (!validOutputPath(outputKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(RecommendReduceFn.class);
    Configuration conf = p.getConfiguration();
    conf.set(Y_KEY_KEY, iterationKey + "Y/");

    PTables.asPTable(p.read(input(tempPrefix + "distributeRecommend/", ALSTypes.REC_TYPE)))
        .groupByKey(groupingOptions())
        .parallelDo("recommend", new RecommendReduceFn(), ALSTypes.VALUE_MATRIX)
        .write(output(outputKey));
    return p;
  }

  @Override
  protected String getCustomJobName() {
    return defaultCustomJobName();
  }

  @Override
  protected boolean isHighMemoryStep() {
    return true;
  }

  public static void main(String[] args) throws Exception {
    run(new RecommendStep(), args);
  }

}
