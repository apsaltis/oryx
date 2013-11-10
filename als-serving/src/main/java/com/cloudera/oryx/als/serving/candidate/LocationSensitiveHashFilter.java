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

package com.cloudera.oryx.als.serving.candidate;

import java.util.Collection;
import java.util.Iterator;

import com.cloudera.oryx.als.common.lsh.LocationSensitiveHash;
import com.cloudera.oryx.common.collection.LongObjectMap;

/**
 * A {@link CandidateFilter} based on location-sensitive hashing, which chooses a set of candidate items
 * to consider by filtering out items that are unlikely to make good recommendations.
 *
 * @author Sean Owen
 */
public final class LocationSensitiveHashFilter implements CandidateFilter {

  private final LocationSensitiveHash delegate;

  public LocationSensitiveHashFilter(LongObjectMap<float[]> Y, double lshSampleRatio, int numHashes) {
    delegate = new LocationSensitiveHash(Y, lshSampleRatio, numHashes);
  }

  @Override
  public Collection<Iterator<LongObjectMap.MapEntry<float[]>>> getCandidateIterator(float[][] userVectors) {
    return delegate.getCandidateIterator(userVectors);
  }

  @Override
  public void addItem(String itemID) {
    delegate.addItem(itemID);
  }

}
