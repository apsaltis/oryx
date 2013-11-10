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
import com.cloudera.oryx.als.common.Rescorer;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.collection.LongFloatMap;

import com.google.common.primitives.Doubles;

/**
 * Used by {@link com.cloudera.oryx.als.common.OryxRecommender#mostPopularItems(int)}.
 *
 * @author Sean Owen
 */
final class MostPopularItemsIterator implements Iterator<NumericIDValue> {

  private final NumericIDValue delegate;
  private final Iterator<LongFloatMap.MapEntry> countsIterator;
  private final Rescorer rescorer;
  private final StringLongMapping idMapping;

  MostPopularItemsIterator(Iterator<LongFloatMap.MapEntry> countsIterator,
                           Rescorer rescorer,
                           StringLongMapping idMapping) {
    delegate = new NumericIDValue();
    this.countsIterator = countsIterator;
    this.rescorer = rescorer;
    this.idMapping = idMapping;
  }

  @Override
  public boolean hasNext() {
    return countsIterator.hasNext();
  }

  @Override
  public NumericIDValue next() {
    LongFloatMap.MapEntry entry = countsIterator.next();
    long id = entry.getKey();
    double value = entry.getValue();
    Rescorer theRescorer = rescorer;
    if (theRescorer != null) {
      String stringID = idMapping.toString(id);
      if (theRescorer.isFiltered(stringID)) {
        return null;
      }
      value = (float) theRescorer.rescore(stringID, value);
      if (!Doubles.isFinite(value)) {
        return null;
      }
    }
    delegate.set(id, (float) value);
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
