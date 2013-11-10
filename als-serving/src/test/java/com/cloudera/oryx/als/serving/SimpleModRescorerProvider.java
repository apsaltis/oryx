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

import com.cloudera.oryx.als.common.Rescorer;
import com.cloudera.oryx.als.common.OryxRecommender;
import com.cloudera.oryx.als.common.PairRescorer;

final class SimpleModRescorerProvider extends AbstractRescorerProvider {
  
  private final int modulus;
  
  SimpleModRescorerProvider(int modulus) {
    this.modulus = modulus;
  }

  @Override
  public Rescorer getRecommendRescorer(String[] userIDs, OryxRecommender recommender, String... args) {
    return userIDs[0].length() % modulus == 0 ? new SimpleModRescorer(modulus) : null;
  }

  @Override
  public Rescorer getRecommendToAnonymousRescorer(String[] itemIDs, OryxRecommender recommender, String... args) {
    return itemIDs[0].length() % modulus == 0 ? new SimpleModRescorer(modulus) : null;
  }

  @Override
  public Rescorer getMostPopularItemsRescorer(OryxRecommender recommender, String... args) {
    return new SimpleModRescorer(modulus);
  }

  @Override
  public PairRescorer getMostSimilarItemsRescorer(OryxRecommender recommender, String... args) {
    return new SimpleModRescorer(modulus);
  }

}
