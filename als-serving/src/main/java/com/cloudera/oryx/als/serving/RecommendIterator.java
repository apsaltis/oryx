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
import com.cloudera.oryx.als.common.Rescorer;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.math.SimpleVectorMath;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;

/**
 * An {@link Iterator} that generates and iterates over all possible candidate items to recommend.
 * It is used to generate recommendations. The items with top values are taken as recommendations.
 *
 * @author Sean Owen
 * @see MostSimilarItemIterator
 * @see RecommendedBecauseIterator
 */
final class RecommendIterator implements Iterator<NumericIDValue> {

  private final NumericIDValue delegate;
  private final float[][] features;
  private final Iterator<LongObjectMap.MapEntry<float[]>> Yiterator;
  private final LongSet knownItemIDs;
  private final Rescorer rescorer;
  private final StringLongMapping idMapping;

  RecommendIterator(float[][] features,
                    Iterator<LongObjectMap.MapEntry<float[]>> Yiterator,
                    LongSet knownItemIDs,
                    Rescorer rescorer,
                    StringLongMapping idMapping) {
    Preconditions.checkArgument(features.length > 0, "features must not be empty");
    delegate = new NumericIDValue();
    this.features = features;
    this.Yiterator = Yiterator;
    this.knownItemIDs = knownItemIDs;
    this.rescorer = rescorer;
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
    
    LongSet theKnownItemIDs = knownItemIDs;
    if (theKnownItemIDs != null) {
      synchronized (theKnownItemIDs) {
        if (theKnownItemIDs.contains(itemID)) {
          return null;
        }
      }
    }

    Rescorer rescorer = this.rescorer;
    if (rescorer != null&& rescorer.isFiltered(idMapping.toString(itemID))) {
      return null;
    }

    float[] itemFeatures = entry.getValue();
    double sum = 0.0;
    int count = 0;
    for (float[] oneUserFeatures : features) {
      sum += SimpleVectorMath.dot(itemFeatures, oneUserFeatures);
      count++;
    }
    
    if (rescorer != null) {
      sum = rescorer.rescore(idMapping.toString(itemID), sum);
      if (!Doubles.isFinite(sum)) {
        return null;
      }
    }

    float result = (float) (sum / count);
    Preconditions.checkState(Floats.isFinite(result), "Bad recommendation value");
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
