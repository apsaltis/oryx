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

package com.cloudera.oryx.als.computation.publish;

import java.io.IOException;

import com.cloudera.oryx.als.computation.types.ALSTypes;
import org.apache.crunch.impl.mr.MRPipeline;

import com.cloudera.oryx.als.computation.iterate.IterationState;
import com.cloudera.oryx.als.computation.iterate.IterationStep;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.computation.common.JobStepConfig;
import org.apache.crunch.types.avro.Avros;

abstract class PublishStep extends IterationStep {

  @Override
  protected final MRPipeline createPipeline() throws IOException {

    JobStepConfig config = getConfig();

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    String xOrY = isX() ? "X/" : "Y/";
    String outputKeyPath =
        Namespaces.getInstanceGenerationPrefix(config.getInstanceDir(), config.getGenerationID()) + xOrY;

    if (!validOutputPath(outputKeyPath)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(PublishMapFn.class);
    p.read(input(iterationKey + xOrY, ALSTypes.DENSE_ROW_MATRIX))
        .parallelDo("publish", new PublishMapFn(), Avros.strings())
        .write(compressedTextOutput(p.getConfiguration(), outputKeyPath));
    return p;
  }

  abstract boolean isX();

  @Override
  protected final String getCustomJobName() {
    return defaultCustomJobName();
  }

}
