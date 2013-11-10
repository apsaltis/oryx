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

/**
 * Simplified enumeration of possible states of a step -- or job -- in a Hadoop job.
 *
 * @author Sean Owen
 */
public enum StepStatus {

  /** Not yet started running. */
  PENDING,
  /** Running normally now. */
  RUNNING,
  /** Stopped running due to an error. */
  FAILED,
  /** Stopped running before completion because of system or user request. */
  CANCELLED,
  /** Completed execution normally and is no longer running. */
  COMPLETED,

}
