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

package com.cloudera.oryx.als.computation.initialy;

import com.cloudera.oryx.als.computation.types.MatrixRow;
import com.cloudera.oryx.common.math.SimpleVectorMath;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.random.RandomUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.fn.OryxReduceMapFn;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.crunch.Pair;

import java.util.List;

public final class InitialYReduceFn extends OryxReduceMapFn<Long, float[], MatrixRow> {

  private static final int MAX_FAR_FROM_VECTORS = 100000;

  private RandomGenerator random;
  private int features;
  private List<float[]> farFrom;

  @Override
  public void initialize() {
    super.initialize();
    random = RandomManager.getRandom();
    Config config = ConfigUtils.getDefaultConfig();
    features = config.getInt("model.features");
    Preconditions.checkArgument(features > 0, "Number of features must be positive");
    farFrom = Lists.newArrayList();
  }

  @Override
  public MatrixRow map(Pair<Long, float[]> input) {
    long itemID = input.first();
    // Did we see any previous feature vector from previous Y?
    float[] featureVector = null;
    float[] maybeFeatureVector = input.second();
    int maybeLength = maybeFeatureVector.length;
    if (maybeLength > 0) {
      if (maybeLength == features) {
        // Only use this vector if not empty and has the right number of features
        featureVector = maybeFeatureVector;
      } else if (maybeLength > features) {
        // Copy part of the existing vector
        featureVector = new float[features];
        System.arraycopy(maybeFeatureVector, 0, featureVector, 0, featureVector.length);
        SimpleVectorMath.normalize(featureVector);
      } else if (maybeLength < features) {
        featureVector = new float[features];
        System.arraycopy(maybeFeatureVector, 0, featureVector, 0, maybeLength);
        for (int i = maybeLength; i < featureVector.length; i++) {
          featureVector[i] = (float) random.nextGaussian();
        }
        SimpleVectorMath.normalize(featureVector);
      }
    }

    if (featureVector == null) {
      // No suitable prior vector, build a new one
      featureVector = RandomUtils.randomUnitVectorFarFrom(features, farFrom, random);
    }

    if (farFrom.size() < MAX_FAR_FROM_VECTORS) { // Simple cap to keep from getting too big
      farFrom.add(featureVector);
    }

    return new MatrixRow(itemID, featureVector);
  }
}
