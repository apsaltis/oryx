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

import com.google.common.collect.Lists;

import com.cloudera.oryx.als.common.PairRescorer;
import com.cloudera.oryx.als.common.Rescorer;
import com.cloudera.oryx.common.ClassUtils;
import com.cloudera.oryx.als.common.OryxRecommender;

/**
 * Abstract implementation of {@link RescorerProvider} which implements all methods to return {@code null}.
 *
 * @author Sean Owen
 */
public abstract class AbstractRescorerProvider implements RescorerProvider {

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getRecommendRescorer(String[] userIDs, OryxRecommender recommender, String... args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getRecommendToAnonymousRescorer(String[] itemIDs, OryxRecommender recommender, String... args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getMostPopularItemsRescorer(OryxRecommender recommender, String... args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public PairRescorer getMostSimilarItemsRescorer(OryxRecommender recommender, String... args) {
    return null;
  }

  /**
   * @param classNamesString a comma-delimited list of class names, where classes implement {@link RescorerProvider}
   * @return a {@link MultiRescorerProvider} which rescores using all of them
   */
  public static RescorerProvider loadRescorerProviders(String classNamesString) {
    if (classNamesString == null || classNamesString.isEmpty()) {
      return null;
    }
    String[] classNames = classNamesString.split(",");
    if (classNames.length == 1) {
      return ClassUtils.loadInstanceOf(classNames[0], RescorerProvider.class);
    }
    List<RescorerProvider> providers = Lists.newArrayListWithCapacity(classNames.length);
    for (String className : classNames) {
      providers.add(ClassUtils.loadInstanceOf(className, RescorerProvider.class));
    }
    return new MultiRescorerProvider(providers);
  }

}
