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

package com.cloudera.oryx.kmeans.serving.generation;

import com.cloudera.oryx.kmeans.common.Centers;
import com.cloudera.oryx.kmeans.common.pmml.KMeansPMML;
import org.apache.commons.math3.linear.RealVector;
import org.dmg.pmml.ClusteringModel;

public final class Generation {

  private final VectorFactory vectorFactory;
  private final Centers centers;

  public Generation(ClusteringModel model) {
    this.vectorFactory = VectorFactory.create(
        model.getMiningSchema(),
        model.getLocalTransformations(),
        model.getClusteringFields());
    this.centers = KMeansPMML.toCenters(model);
  }

  public RealVector toVector(String[] tokens) {
    return vectorFactory.createVector(tokens);
  }

  public Centers getCentroids() {
    return centers;
  }

}
