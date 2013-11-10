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
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mr.MRPipeline;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.common.servcomp.Namespaces;

/**
 * @author Sean Owen
 */
abstract class AbstractToVectorsStep extends ALSJobStep {

  @Override
  protected final MRPipeline createPipeline() throws IOException {

    JobStepConfig config = getConfig();
    String instanceDir = config.getInstanceDir();
    long generationID = config.getGenerationID();

    String inputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "input/";
    String outputKey = Namespaces.getTempPrefix(instanceDir, generationID) + getSuffix();
    if (!validOutputPath(outputKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(ToVectorReduceFn.class);
    getMatrix(p, inputKey)
        .groupByKey(groupingOptions())
        .parallelDo("toVectors", new ToVectorReduceFn(), ALSTypes.SPARSE_ROW_MATRIX)
        .write(output(outputKey));
    return p;
 }
  
  abstract PTable<Long, NumericIDValue> getMatrix(MRPipeline pipeline, String inputKey);
  
  abstract String getSuffix();

}
