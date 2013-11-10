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
import java.util.Date;

import com.google.common.base.Preconditions;

/**
 * Encapsulates the status of a single step's state.
 *
 * @author Sean Owen
 */
public final class StepState implements Serializable {

  private final Date startTime;
  private final Date endTime;
  private final String name;
  private final StepStatus status;
  private float mapProgress;
  private float reduceProgress;

  StepState(Date startTime,
            Date endTime,
            String name,
            StepStatus status) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(status);
    this.startTime = clone(startTime);
    this.endTime = clone(endTime);
    this.name = name;
    this.status = status;
    mapProgress = Float.NaN;
    reduceProgress = Float.NaN;
  }

  private static Date clone(Date d) {
    return d == null ? null : new Date(d.getTime());
  }

  /**
   * @return time that the step started or {@code null} if not started
   */
  public Date getStartTime() {
    return clone(startTime);
  }

  /**
   * @return time that the step ended or {@code null} if not completed
   */
  public Date getEndTime() {
    return clone(endTime);
  }

  /**
   * @return the name of the step, as used by the cluster
   */
  public String getName() {
    return name;
  }

  /**
   * @return current status of the step, like {@link StepStatus#RUNNING}
   */
  public StepStatus getStatus() {
    return status;
  }

  /**
   * @return step's mapper progress, as value in [0,1]
   */
  public float getMapProgress() {
    return mapProgress;
  }

  void setMapProgress(float mapProgress) {
    this.mapProgress = mapProgress;
  }

  /**
   * @return step's reducer progress, as value in [0,1]
   */
  public float getReduceProgress() {
    return reduceProgress;
  }

  void setReduceProgress(float reduceProgress) {
    this.reduceProgress = reduceProgress;
  }

  @Override
  public String toString() {
    return name + ':' + status;
  }

}
