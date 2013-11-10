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

import java.util.concurrent.locks.Lock;

import com.cloudera.oryx.common.ClassUtils;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * <p>This class helps choose which {@link CandidateFilter} to apply to the recommendation process.
 * If the "model.candidateFilter.customClass" system property is set, then this class will be loaded and used.
 * See notes in {@link CandidateFilter} about how the class must be implemented.</p>
 * 
 * <p>Otherwise, if "model.lsh.sample-ratio" is set to a value less than 1, then {@link LocationSensitiveHashFilter}
 * will be used. It is a somewhat special case, a built-in type of filter.</p>
 * 
 * <p>Otherwise an implementation that does no filtering will be returned.</p>
 * 
 * @author Sean Owen
 */
public final class CandidateFilterFactory {

  private final double lshSampleRatio;
  private final int numHashes;
  private final String candidateFilterClassName;

  public CandidateFilterFactory() {
    Config config = ConfigUtils.getDefaultConfig();
    lshSampleRatio = config.getDouble("model.lsh.sample-ratio");
    numHashes = config.getInt("model.lsh.num-hashes");
    candidateFilterClassName =
        config.hasPath("serving-layer.candidate-filter-class") ?
        config.getString("serving-layer.candidate-filter-class") : null;
  }

  /**
   * @return an implementation of {@link CandidateFilter} chosen per above. It will be non-null.
   * 
   * @param Y item-feature matrix
   * @param yReadLock read lock that should be acquired to access {@code Y}
   */
  public CandidateFilter buildCandidateFilter(LongObjectMap<float[]> Y, Lock yReadLock) {
    Preconditions.checkNotNull(Y);
    if (!Y.isEmpty()) {
      yReadLock.lock();
      try {
        if (candidateFilterClassName != null) {
          return ClassUtils.loadInstanceOf(candidateFilterClassName,
                                           CandidateFilter.class,
                                           new Class<?>[]{LongObjectMap.class},
                                           new Object[]{Y});
        }
        // LSH is a bit of a special case, handled here
        if (lshSampleRatio < 1.0) {
          return new LocationSensitiveHashFilter(Y, lshSampleRatio, numHashes);
        }
      } finally {
        yReadLock.unlock();
      }
    }
    return new IdentityCandidateFilter(Y);    
  }
  
}
