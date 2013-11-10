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

import com.google.common.base.Preconditions;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.common.PairRescorer;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.math.SimpleVectorMath;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;

/**
 * An {@link Iterator} that generates and iterates over all possible candidate items in computation
 * of {@link com.cloudera.oryx.als.common.OryxRecommender#mostSimilarItems(String, int)}.
 *
 * @author Sean Owen
 * @see RecommendedBecauseIterator
 * @see RecommendIterator
 */
final class MostSimilarItemIterator implements Iterator<NumericIDValue> {

  private final NumericIDValue delegate;
  private final float[][] itemFeatures;
  private final double[] itemFeatureNorms;
  private final Iterator<LongObjectMap.MapEntry<float[]>> Yiterator;
  private final long[] toItemIDs;
  private final PairRescorer rescorer;
  private final StringLongMapping idMapping;

  MostSimilarItemIterator(Iterator<LongObjectMap.MapEntry<float[]>> Yiterator,
                          long[] toItemIDs,
                          float[][] itemFeatures,
                          PairRescorer rescorer,
                          StringLongMapping idMapping) {
    delegate = new NumericIDValue();
    this.toItemIDs = toItemIDs;
    this.itemFeatures = itemFeatures;
    this.Yiterator = Yiterator;
    this.rescorer = rescorer;
    itemFeatureNorms = new double[itemFeatures.length];
    for (int i = 0; i < itemFeatures.length; i++) {
      itemFeatureNorms[i] = SimpleVectorMath.norm(itemFeatures[i]);
    }
    this.idMapping = idMapping;
  }

  @Override
  public boolean hasNext() {
    return Yiterator.hasNext();
  }

  @Override
  public NumericIDValue next() {
    LongObjectMap.MapEntry<float[]> entry = Yiterator.next();
    long itemID = entry.getKey();
    
    for (long l : toItemIDs) {
      if (l == itemID) {
        return null;
      }
    }

    PairRescorer rescorer1 = this.rescorer;
    float[] candidateFeatures = entry.getValue();
    double candidateFeaturesNorm = SimpleVectorMath.norm(candidateFeatures);
    double total = 0.0;

    int length = itemFeatures.length;
    for (int i = 0; i < length; i++) {
      long toItemID = toItemIDs[i];
      if (rescorer1 != null && rescorer1.isFiltered(idMapping.toString(itemID), idMapping.toString(toItemID))) {
        return null;
      }
      double similarity = SimpleVectorMath.dot(candidateFeatures, itemFeatures[i]) / 
          (candidateFeaturesNorm * itemFeatureNorms[i]);
      if (!Doubles.isFinite(similarity)) {
        return null;
      }
      if (rescorer1 != null) {
        similarity = rescorer1.rescore(idMapping.toString(itemID), idMapping.toString(toItemID), similarity);
        if (!Doubles.isFinite(similarity)) {
          return null;
        }
      }
      total += similarity;
    }

    float result = (float) (total / length);
    Preconditions.checkState(Floats.isFinite(result), "Bad similarity value");
    delegate.set(itemID, result);
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
