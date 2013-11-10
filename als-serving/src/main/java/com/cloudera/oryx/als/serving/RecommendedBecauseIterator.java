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

package com.cloudera.oryx.als.serving;

import java.util.Iterator;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.math.SimpleVectorMath;
import com.google.common.primitives.Doubles;

/**
 * An {@link Iterator} that generates and iterates over all possible candidate items in computation
 * of {@link com.cloudera.oryx.als.common.OryxRecommender#recommendedBecause(String, String, int)}.
 *
 * @author Sean Owen
 * @see MostSimilarItemIterator
 * @see RecommendIterator
 */
final class RecommendedBecauseIterator implements Iterator<NumericIDValue> {

  private final NumericIDValue delegate;
  private final float[] features;
  private final double featuresNorm;
  private final Iterator<LongObjectMap.MapEntry<float[]>> toFeaturesIterator;

  RecommendedBecauseIterator(Iterator<LongObjectMap.MapEntry<float[]>> toFeaturesIterator,
                             float[] features) {
    delegate = new NumericIDValue();
    this.features = features;
    this.featuresNorm = SimpleVectorMath.norm(features);
    this.toFeaturesIterator = toFeaturesIterator;
  }

  @Override
  public boolean hasNext() {
    return toFeaturesIterator.hasNext();
  }

  @Override
  public NumericIDValue next() {
    LongObjectMap.MapEntry<float[]> entry = toFeaturesIterator.next();
    long itemID = entry.getKey();
    float[] candidateFeatures = entry.getValue();
    double candidateFeaturesNorm = SimpleVectorMath.norm(candidateFeatures);
    double estimate = SimpleVectorMath.dot(candidateFeatures, features) / (candidateFeaturesNorm * featuresNorm);
    if (!Doubles.isFinite(estimate)) {
      return null;
    }
    delegate.set(itemID, (float) estimate);
    return delegate;
  }

  /**
   * @throws UnsupportedOperationException
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
