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
import com.cloudera.oryx.als.computation.types.MatrixRow;
import com.cloudera.oryx.common.collection.LongSet;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.types.avro.Avros;

import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.als.computation.iterate.IterationState;
import com.cloudera.oryx.als.computation.iterate.IterationStep;
import com.cloudera.oryx.common.servcomp.Namespaces;

/**
 * @author Sean Owen
 */
public final class DistributeRecommendWorkStep extends IterationStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    JobStepConfig config = getConfig();
    String instanceDir = config.getInstanceDir();
    long generationID = config.getGenerationID();

    String outputPathKey = Namespaces.getTempPrefix(instanceDir, generationID) + "distributeRecommend/";
    if (!validOutputPath(outputPathKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(DistributeRecommendWorkFn.class);

    String knownItemsKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "knownItems/";
    PTable<Long, LongSet> knownItems = p.read(textInput(knownItemsKey))
        .parallelDo("knownItems", new KnownItemsFn(), Avros.tableOf(ALSTypes.LONGS, ALSTypes.ID_SET));
    PTable<Long, float[]> userFeatures = p.read(input(iterationKey + "X/", ALSTypes.DENSE_ROW_MATRIX))
        .parallelDo("asPair", MatrixRow.AS_PAIR, Avros.tableOf(Avros.longs(), ALSTypes.FLOAT_ARRAY));

    JoinStrategy<Long, float[], LongSet> joinStrategy = new DefaultJoinStrategy<Long, float[], LongSet>(
        getNumReducers());
    PTable<Long, Pair<float[], LongSet>> joined = joinStrategy.join(userFeatures, knownItems, JoinType.INNER_JOIN);

    joined.parallelDo(
        "distribute", new DistributeRecommendWorkFn(), ALSTypes.REC_TYPE)
        .write(output(outputPathKey));

    return p;
  }

  @Override
  protected String getCustomJobName() {
    return defaultCustomJobName();
  }


  public static void main(String[] args) throws Exception {
    run(new DistributeRecommendWorkStep(), args);
  }

}
