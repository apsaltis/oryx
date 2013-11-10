/**
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
package com.cloudera.oryx.kmeans.computation.covariance;

import com.google.common.collect.Maps;
import org.apache.commons.math3.linear.RealVector;

import java.util.Map;
import java.util.Set;

final class CoMomentTracker {
  private final Map<Index, CoMoment> cache = Maps.newHashMap();

  public void reset() {
    cache.clear();
  }

  public void update(RealVector v) {
    for (int i = 0; i < v.getDimension(); i++) {
      double vi = v.getEntry(i);
      for (int j = i; j < v.getDimension(); j++) {
        Index idx = new Index(i, j);
        CoMoment cm = cache.get(idx);
        if (cm == null) {
          cm = new CoMoment();
          cache.put(idx, cm);
        }
        cm.update(vi, v.getEntry(j));
      }
    }
  }

  public Set<Map.Entry<Index, CoMoment>> entrySet() {
    return cache.entrySet();
  }
}
