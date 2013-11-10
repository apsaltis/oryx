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

package com.cloudera.oryx.kmeans.computation;

import com.cloudera.oryx.computation.common.JobStepConfig;

public final class KMeansJobStepConfig extends JobStepConfig {

  public KMeansJobStepConfig(String instanceDir,
                             long generationID,
                             long lastGenerationID,
                             int iteration) {
    super(instanceDir, generationID, lastGenerationID, iteration);
  }

  @Override
  public String[] toArgsArray() {
    return new String[] {
        getInstanceDir(),
        Long.toString(getGenerationID()),
        Long.toString(getLastGenerationID()),
        Integer.toString(getIteration()),
    };
  }
}
