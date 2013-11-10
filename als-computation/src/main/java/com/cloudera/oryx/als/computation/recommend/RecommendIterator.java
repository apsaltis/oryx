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

package com.cloudera.oryx.als.computation.recommend;

import java.util.Iterator;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.math.SimpleVectorMath;

/**
 * An {@link Iterator} that generates and iterates over all possible candidate items to recommend.
 * It is used to generate recommendations. The items with top values are taken as recommendations.
 *
 * @author Sean Owen
 */
public final class RecommendIterator implements Iterator<NumericIDValue> {

  private final NumericIDValue delegate;
  private final float[] features;
  private final Iterator<LongObjectMap.MapEntry<float[]>> Yiterator;
  private final LongSet knownItemIDs;

  public RecommendIterator(float[] features,
                           Iterator<LongObjectMap.MapEntry<float[]>> Yiterator,
                           LongSet knownItemIDs) {
    Preconditions.checkArgument(features.length > 0, "Feature vector can't be empty");
    delegate = new NumericIDValue();
    this.features = features;
    this.Yiterator = Yiterator;
    this.knownItemIDs = knownItemIDs;
  }

  @Override
  public boolean hasNext() {
    return Yiterator.hasNext();
  }

  @Override
  public NumericIDValue next() {
    LongObjectMap.MapEntry<float[]> entry = Yiterator.next();
    long itemID = entry.getKey();
    if (knownItemIDs.contains(itemID)) {
      return null;
    }
    delegate.set(itemID, (float) SimpleVectorMath.dot(entry.getValue(), features));
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
