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

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

/**
 * Encapsulates the current state of a run of {@link DistributedGenerationRunner}.
 *
 * @author Sean Owen
 */
public final class GenerationRunnerState implements Serializable {

  private final long generationID;
  private final List<StepState> stepStates;
  private final boolean running;
  private final Date startTime;
  private final Date endTime;

  public GenerationRunnerState(long generationID,
                               List<StepState> stepStates,
                               boolean running,
                               Date startTime,
                               Date endTime) {
    this.generationID = generationID;
    this.stepStates = stepStates;
    this.running = running;
    this.startTime = clone(startTime);
    this.endTime = clone(endTime);
  }

  private static Date clone(Date d) {
    return d == null ? null : new Date(d.getTime());
  }

  public long getGenerationID() {
    return generationID;
  }

  public boolean isRunning() {
    return running;
  }

  public List<StepState> getStepStates() {
    return stepStates;
  }

  public StepStatus getStatus() {
    if (stepStates.isEmpty()) {
      return StepStatus.PENDING;
    }
    Collection<StepStatus> observedStates = EnumSet.noneOf(StepStatus.class);
    for (StepState stepState : stepStates) {
      observedStates.add(stepState.getStatus());
    }
    for (StepStatus clearStatuses :
        new StepStatus[] { StepStatus.FAILED, StepStatus.CANCELLED, StepStatus.RUNNING }) {
      if (observedStates.contains(clearStatuses)) {
        return clearStatuses;
      }
    }
    // Only pending and completed now
    if (observedStates.contains(StepStatus.PENDING)) {
      if (observedStates.contains(StepStatus.COMPLETED)) {
        return StepStatus.RUNNING;
      }
      return StepStatus.PENDING;
    }
    return StepStatus.COMPLETED;
  }
  /**
   * @return time that the generation start or {@code null} if not started
   */
  public Date getStartTime() {
    return clone(startTime);
  }

  /**
   * @return time that the generation ended or {@code null} if not completed
   */
  public Date getEndTime() {
    return clone(endTime);
  }

  @Override
  public String toString() {
    return generationID + ":" + stepStates;
  }

}
