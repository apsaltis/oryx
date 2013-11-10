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

/**
 * Encapsulates the parameters of an iteration step, like which iteration number it is.
 * 
 * @author Sean Owen
 */
public final class IterationState {
  
  private final boolean computingX;
  private final int iteration;
  private final String iterationKey;
  
  IterationState(boolean computingX, int iteration, String iterationKey) {
    this.computingX = computingX;
    this.iteration = iteration;
    this.iterationKey = iterationKey;
  }

  public boolean isComputingX() {
    return computingX;
  }

  public int getIteration() {
    return iteration;
  }
  
  public String getIterationKey() {
    return iterationKey;
  }
  
  @Override
  public String toString() {
    return (computingX ? 'X' : 'Y') + " / " + iteration + " / " + iterationKey;
  }

}
