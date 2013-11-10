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

package com.cloudera.oryx.als.computation;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.computation.common.JobStepConfig;

public final class ALSJobStepConfig extends JobStepConfig {

  private final boolean computingX;

  public ALSJobStepConfig(String instanceDir,
                          long generationID,
                          long lastGenerationID,
                          int iteration,
                          boolean computingX) {
    super(instanceDir, generationID, lastGenerationID, iteration);
    this.computingX = computingX;
  }

  public boolean isComputingX() {
    return computingX;
  }

  @Override
  public String[] toArgsArray() {
    return new String[] {
        getInstanceDir(),
        Long.toString(getGenerationID()),
        Long.toString(getLastGenerationID()),
        Integer.toString(getIteration()),
        Boolean.toString(computingX),
    };
  }

  public static ALSJobStepConfig fromArgsArray(String... args) {
    Preconditions.checkNotNull(args);
    Preconditions.checkArgument(args.length >= 5);
    return new ALSJobStepConfig(args[0],
                                Long.parseLong(args[1]),
                                Long.parseLong(args[2]),
                                Integer.parseInt(args[3]),
                                Boolean.parseBoolean(args[4]));
  }

}
