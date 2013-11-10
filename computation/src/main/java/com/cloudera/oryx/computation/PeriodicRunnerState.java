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

package com.cloudera.oryx.computation;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import com.cloudera.oryx.computation.common.GenerationRunnerState;

/**
 * Encapsulates the current state of a run of {@link PeriodicRunner}.
 *
 * @author Sean Owen
 */
public final class PeriodicRunnerState implements Serializable {

  private final List<GenerationRunnerState> generationStates;
  private final boolean running;
  private final Date nextScheduledRun;
  private final int currentGenerationMB;

  PeriodicRunnerState(List<GenerationRunnerState> generationStates,
                      boolean running,
                      Date nextScheduledRun,
                      int currentGenerationMB) {
    this.generationStates = generationStates;
    this.running = running;
    this.nextScheduledRun = clone(nextScheduledRun);
    this.currentGenerationMB = currentGenerationMB;
  }

  private static Date clone(Date d) {
    return d == null ? null : new Date(d.getTime());
  }

  public List<GenerationRunnerState> getGenerationStates() {
    return generationStates;
  }

  public boolean isRunning() {
    return running;
  }

  /**
   * @return time when next run is scheduled to start, or {@code null} if not scheduled
   */
  public Date getNextScheduledRun() {
    return clone(nextScheduledRun);
  }

  public int getCurrentGenerationMB() {
    return currentGenerationMB;
  }

  @Override
  public String toString() {
    return generationStates.toString();
  }

}
