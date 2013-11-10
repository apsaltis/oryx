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

import java.io.IOException;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.computation.types.ALSTypes;
import com.google.common.base.Preconditions;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.join.JoinUtils;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.Avros;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;

/**
 * @author Sean Owen
 */
public final class MergeNewOldStep extends ALSJobStep {

  private static final PTableType<Pair<Long, Integer>, NumericIDValue> JOIN_TYPE = Avros.tableOf(
      Avros.pairs(ALSTypes.LONGS, ALSTypes.INTS),
      ALSTypes.IDVALUE);

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

    MRPipeline p = createBasicPipeline(MergeNewOldValuesFn.class);

    String inboundKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "inbound/";

    PCollection<Pair<Long, NumericIDValue>> parsed = p.read(textInput(inboundKey))
        .parallelDo("inboundParse", new DelimitedInputParseFn(),
            Avros.pairs(ALSTypes.LONGS, ALSTypes.IDVALUE));

    PTable<Pair<Long, Integer>, NumericIDValue> inbound = parsed.parallelDo("inbound", new InboundJoinFn(), JOIN_TYPE);

    if (lastGenerationID >= 0) {
      String inputPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "input/";
      Preconditions.checkState(Store.get().exists(inputPrefix, false), "Input path does not exist: %s", inputPrefix);
      PTable<Pair<Long, Integer>, NumericIDValue> joinBefore = p.read(input(inputPrefix, ALSTypes.VALUE_MATRIX))
          .parallelDo("lastGeneration", new JoinBeforeMapFn(), JOIN_TYPE);
      inbound = inbound.union(joinBefore);
    }

    GroupingOptions groupingOptions = GroupingOptions.builder()
        .partitionerClass(JoinUtils.getPartitionerClass(inbound.getTypeFamily()))
        .numReducers(getNumReducers())
        .build();

    inbound
        .groupByKey(groupingOptions)
        .parallelDo(new MergeNewOldValuesFn(), ALSTypes.VALUE_MATRIX)
        .write(output(outputKey));
    return p;
  }

  public static void main(String[] args) throws Exception {
    run(new MergeNewOldStep(), args);
  }

}
