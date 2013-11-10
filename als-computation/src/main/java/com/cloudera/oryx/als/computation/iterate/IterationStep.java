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

package com.cloudera.oryx.als.computation.iterate;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.als.computation.ALSJobStepConfig;
import com.cloudera.oryx.computation.common.JobStep;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.computation.common.JobStepConfig;

/**
 * Superclass of {@link JobStep}s that are run many times, iteratively
 * 
 * @author Sean Owen
 */
public abstract class IterationStep extends ALSJobStep {

  private IterationState iterationState;

  @Override
  protected String getCustomJobName() {
    IterationState iterationState = getIterationState();
    StringBuilder name = new StringBuilder(100);
    JobStepConfig config = getConfig();
    name.append("Oryx-").append(config.getInstanceDir());
    name.append('-').append(config.getGenerationID());
    name.append('-').append(iterationState.getIteration());
    name.append('-').append(iterationState.isComputingX() ? 'X' : 'Y');
    name.append('-').append(getClass().getSimpleName());
    return name.toString();
  }

  protected final IterationState getIterationState() {
    if (iterationState != null) {
      return iterationState;
    }
    ALSJobStepConfig config = getConfig();
    int currentIteration = config.getIteration();
    String iterationsKey =
        Namespaces.getIterationsPrefix(config.getInstanceDir(), config.getGenerationID()) +
        currentIteration + '/';
    iterationState = new IterationState(config.isComputingX(), currentIteration, iterationsKey);
    return iterationState;
  }
  
}
