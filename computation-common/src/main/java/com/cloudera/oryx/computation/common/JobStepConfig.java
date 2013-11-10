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

package com.cloudera.oryx.computation.common;

import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * Parameters that are sent to a {@link JobStep} when it executes against a Hadoop cluster.
 * 
 * @author Sean Owen
 */
public abstract class JobStepConfig {
  
  private final String instanceDir;
  private final long generationID;
  private final long lastGenerationID;
  private final int iteration;

  protected JobStepConfig(String instanceDir,
                          long generationID,
                          long lastGenerationID,
                          int iteration) {
    Preconditions.checkNotNull(instanceDir);
    Preconditions.checkArgument(generationID >= 0L, "Generation must be nonnegative: {}", generationID);
    Preconditions.checkArgument(iteration >= 0, "Iteration must be nonnegative: {}", iteration);
    this.instanceDir = instanceDir;
    this.generationID = generationID;
    this.lastGenerationID = lastGenerationID;
    this.iteration = iteration;
  }

  public final String getInstanceDir() {
    return instanceDir;
  }

  public final long getGenerationID() {
    return generationID;
  }

  public final long getLastGenerationID() {
    return lastGenerationID;
  }

  public final int getIteration() {
    return iteration;
  }
  
  public abstract String[] toArgsArray();
  
  @Override
  public final String toString() {
    return Arrays.toString(toArgsArray());
  }

}
