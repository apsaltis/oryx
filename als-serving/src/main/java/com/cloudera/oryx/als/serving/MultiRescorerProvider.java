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

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.cloudera.oryx.als.common.Rescorer;
import com.cloudera.oryx.als.common.OryxRecommender;
import com.cloudera.oryx.als.common.PairRescorer;

/**
 * Convenience implementation that will aggregate the behavior of multiple {@link RescorerProvider}s.
 * It will filter an item if any of the given instances filter it, and will rescore by applying
 * the rescorings in the given order.
 *
 * @author Sean Owen
 * @see MultiRescorer
 * @see MultiLongPairRescorer
 */
public final class MultiRescorerProvider extends AbstractRescorerProvider {
  
  private final RescorerProvider[] providers;
  
  public MultiRescorerProvider(RescorerProvider... providers) {
    Preconditions.checkNotNull(providers);
    Preconditions.checkArgument(providers.length > 0, "providers is empty");
    this.providers = providers;
  }
  
  public MultiRescorerProvider(List<RescorerProvider> providers) {
    Preconditions.checkNotNull(providers);
    Preconditions.checkArgument(!providers.isEmpty());
    this.providers = providers.toArray(new RescorerProvider[providers.size()]);
  }
  
  @Override
  public Rescorer getRecommendRescorer(String[] userIDs, OryxRecommender recommender, String... args) {
    List<Rescorer> rescorers = Lists.newArrayListWithCapacity(providers.length);
    for (RescorerProvider provider : providers) {
      Rescorer rescorer = provider.getRecommendRescorer(userIDs, recommender, args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    return buildRescorer(rescorers);
  }

  @Override
  public Rescorer getRecommendToAnonymousRescorer(String[] itemIDs, OryxRecommender recommender, String... args) {
    List<Rescorer> rescorers = Lists.newArrayListWithCapacity(providers.length);
    for (RescorerProvider provider : providers) {
      Rescorer rescorer = provider.getRecommendToAnonymousRescorer(itemIDs, recommender, args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    return buildRescorer(rescorers);  
  }

  @Override
  public Rescorer getMostPopularItemsRescorer(OryxRecommender recommender, String... args) {
    List<Rescorer> rescorers = Lists.newArrayListWithCapacity(providers.length);
    for (RescorerProvider provider : providers) {
      Rescorer rescorer = provider.getMostPopularItemsRescorer(recommender, args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    return buildRescorer(rescorers); 
  }
  
  private static Rescorer buildRescorer(List<Rescorer> rescorers) {
    int numRescorers = rescorers.size();
    if (numRescorers == 0) {
      return null;
    }
    if (numRescorers == 1) {
      return rescorers.get(0);
    }
    return new MultiRescorer(rescorers);
  }

  @Override
  public PairRescorer getMostSimilarItemsRescorer(OryxRecommender recommender, String... args) {
    List<PairRescorer> rescorers = Lists.newArrayListWithCapacity(providers.length);
    for (RescorerProvider provider : providers) {
      PairRescorer rescorer = provider.getMostSimilarItemsRescorer(recommender, args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    int numRescorers = rescorers.size();
    if (numRescorers == 0) {
      return null;
    }
    if (numRescorers == 1) {
      return rescorers.get(0);
    }
    return new MultiLongPairRescorer(rescorers);
  }

}
