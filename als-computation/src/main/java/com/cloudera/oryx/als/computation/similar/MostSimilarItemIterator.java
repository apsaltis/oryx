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

package com.cloudera.oryx.als.computation.similar;

import java.util.Iterator;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.math.SimpleVectorMath;

import com.google.common.primitives.Doubles;

public final class MostSimilarItemIterator implements Iterator<NumericIDValue> {

  private final NumericIDValue delegate;
  private final float[] itemFeatures;
  private final double itemFeaturesNorm;
  private final Iterator<LongObjectMap.MapEntry<float[]>> Yiterator;
  private final long toItemID;

  public MostSimilarItemIterator(Iterator<LongObjectMap.MapEntry<float[]>> Yiterator,
                                 long toItemID,
                                 float[] itemFeatures) {
    delegate = new NumericIDValue();
    this.toItemID = toItemID;
    this.itemFeatures = itemFeatures;
    this.itemFeaturesNorm = SimpleVectorMath.norm(itemFeatures);
    this.Yiterator = Yiterator;
  }

  @Override
  public boolean hasNext() {
    return Yiterator.hasNext();
  }

  @Override
  public NumericIDValue next() {
    LongObjectMap.MapEntry<float[]> entry = Yiterator.next();
    long itemID = entry.getKey();
    if (toItemID == itemID) {
      return null;
    }
    float[] candidateFeatures = entry.getValue();
    double candidateFeaturesNorm = SimpleVectorMath.norm(candidateFeatures);
    double similarity =
        SimpleVectorMath.dot(itemFeatures, candidateFeatures) / (itemFeaturesNorm * candidateFeaturesNorm);
    if (!Doubles.isFinite(similarity)) {
      return null;
    }
    delegate.set(itemID, (float) similarity);
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
