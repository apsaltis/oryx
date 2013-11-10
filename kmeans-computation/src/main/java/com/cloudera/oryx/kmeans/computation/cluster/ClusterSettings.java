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

package com.cloudera.oryx.kmeans.computation.cluster;

import com.typesafe.config.Config;

import java.io.Serializable;

public final class ClusterSettings implements Serializable {

  private final int crossFolds;
  private final int sketchIterations;
  private final int sketchPoints;
  private final int indexBits;
  private final int indexSamples;

  public static ClusterSettings create(Config config) {
    Config kmeans = config.getConfig("model");
    int crossFolds = kmeans.getInt("cross-folds");
    int sketchIterations = kmeans.getInt("sketch-iterations");
    int sketchPoints = kmeans.getInt("sketch-points");
    int indexBits = kmeans.getInt("index-bits");
    int indexSamples = kmeans.getInt("index-samples");

    return new ClusterSettings(crossFolds, sketchIterations, sketchPoints, indexBits, indexSamples);
  }

  private ClusterSettings(int crossFolds, int sketchIterations, int sketchPoints, int indexBits, int indexSamples) {
    this.crossFolds = crossFolds;
    this.sketchIterations = sketchIterations;
    this.sketchPoints = sketchPoints;
    this.indexBits = indexBits;
    this.indexSamples = indexSamples;
  }

  public int getCrossFolds() {
    return crossFolds;
  }

  public int getSketchIterations() {
    return sketchIterations;
  }

  public int getSketchPoints() {
    return sketchPoints;
  }

  public int getTotalPoints() {
    return sketchIterations * sketchPoints + 1;
  }

  public int getIndexBits() {
    return indexBits;
  }

  public int getIndexSamples() {
    return indexSamples;
  }
}
