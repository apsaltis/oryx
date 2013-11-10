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

package com.cloudera.oryx.als.computation.popular;

import java.io.IOException;

import com.cloudera.oryx.als.computation.types.ALSTypes;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.common.servcomp.Namespaces;

/**
 * @author Sean Owen
 */
abstract class AbstractPopularStep extends ALSJobStep {

  @Override
  protected final MRPipeline createPipeline() throws IOException {
    JobStepConfig config = getConfig();
    String tempPrefix = Namespaces.getTempPrefix(config.getInstanceDir(), config.getGenerationID());
    String outputPathKey = tempPrefix + getPopularPathDir() + '/';
    if (!validOutputPath(outputPathKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(PopularMapFn.class);
    p.read(input(tempPrefix + getSourceDir() + '/', ALSTypes.SPARSE_ROW_MATRIX))
        .parallelDo("popularMap", new PopularMapFn(), Avros.tableOf(ALSTypes.INTS, ALSTypes.ID_SET))
        .groupByKey(groupingOptions())
        //.combineValues(new FastIDSetAggregator())
        .parallelDo("popularReduce", new PopularReduceFn(), ALSTypes.LONGS)
        .write(output(outputPathKey));
    return p;
  }

  abstract String getSourceDir();
  
  abstract String getPopularPathDir();

}
