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

/**
 * <p>Implementations of this interface provide, optionally, objects that can be used to modify and influence
 * the results of:</p>
 *
 * <ul>
 *  <li>{@link ServerRecommender#recommend(String, int)}</li>
 *  <li>{@link ServerRecommender#recommendToMany(String[], int, boolean, Rescorer)}</li>
 *  <li>{@link ServerRecommender#recommendToAnonymous(String[], int, Rescorer)}</li>
 *  <li>{@link ServerRecommender#mostSimilarItems(String, int, PairRescorer)}</li>
 * </ul>
 *
 * <p>It is a means to inject business logic into the results of {@link ServerRecommender}.</p>
 *
 * <p>Implementations of this class are factories. An implementation creates and configures an {@link Rescorer}
 * rescoring object and returns it for use in the context of one
 * {@link ServerRecommender#recommend(String, int, Rescorer)} method call. (A {@code Rescorer&lt;LongPair&gt;}
 * is used for {@link ServerRecommender#mostSimilarItems(String, int, PairRescorer)} since it operates on item ID
 * <em>pairs</em>, but is otherwise analogous.) The {@link Rescorer} then filters the candidates
 * recommendations or most similar items by item ID ({@link Rescorer#isFiltered(String)})
 * or modifies the scores of item candidates that are not filtered ({@link Rescorer#rescore(String, double)})
 * based on the item ID and original score.</p>
 *
 * <p>The factory methods, like {@link #getRecommendRescorer(String[], OryxRecommender, String...)}, take optional
 * {@code String} arguments. These are passed from the REST API, as a {@code String}, from URL parameter
 * {@code rescorerParams}. The implementation may need this information to initialize its rescoring
 * logic for the request.  For example, the argument may be the user's current location, used to filter
 * results by location.</p>
 *
 * <p>For example, a request containing {@code ...?rescorerParams=xxx,yyy,zzz} will result in an {@code args}
 * parameter with <em>one</em> elements, {@code xxx,yyy,zzz}. A request containing 
 * {@code ...?rescorerParams=xxx&...rescorerParams=yyy&...rescorerParams=zzz...} will result in an
 * {@code args} parameter with 3 elements, {@code xxx}, {@code yyy}, {@code zzz}.</p>
 *
 * @author Sean Owen
 * @see MultiRescorer
 * @see com.cloudera.oryx.als.serving.candidate.CandidateFilter
 */
public interface RescorerProvider {

  /**
   * @param userIDs user(s) for which recommendations are being made, which may be needed in the rescoring logic.
   * @param recommender the recommender instance that is rescoring results
   * @param args arguments, if any, that should be used when making the {@link Rescorer}. This is additional
   *  information from the request that may be necessary to its logic, like current location. What it means
   *  is up to the implementation.
   * @return {@link Rescorer} to use with {@link ServerRecommender#recommend(String, int, Rescorer)}
   *  or {@code null} if none should be used. The resulting {@link Rescorer} will be passed each candidate
   *  item ID to {@link Rescorer#isFiltered(String)}, and each non-filtered candidate with its original score
   *  to {@link Rescorer#rescore(String, double)}
   */
  Rescorer getRecommendRescorer(String[] userIDs, OryxRecommender recommender, String... args);

  /**
   * @param itemIDs items that the anonymous user is associated to
   * @param recommender the recommender instance that is rescoring results
   * @param args arguments, if any, that should be used when making the {@link Rescorer}. This is additional
   *  information from the request that may be necessary to its logic, like current location. What it means
   *  is up to the implementation.
   * @return {@link Rescorer} to use with {@link ServerRecommender#recommendToAnonymous(String[], int, Rescorer)}
   *  or {@code null} if none should be used. The resulting {@link Rescorer} will be passed each candidate
   *  item ID to {@link Rescorer#isFiltered(String)}, and each non-filtered candidate with its original score
   *  to {@link Rescorer#rescore(String, double)}
   */
  Rescorer getRecommendToAnonymousRescorer(String[] itemIDs, OryxRecommender recommender, String... args);

  /**
   * @param recommender the recommender instance that is rescoring results
   * @param args arguments, if any, that should be used when making the {@link Rescorer}. This is additional
   *  information from the request that may be necessary to its logic, like current location. What it means
   *  is up to the implementation.
   * @return {@link Rescorer} to use with {@link ServerRecommender#mostPopularItems(int, Rescorer)}
   *  or {@code null} if none should be used.
   */
  Rescorer getMostPopularItemsRescorer(OryxRecommender recommender, String... args);

  /**
   * @param recommender the recommender instance that is rescoring results
   * @param args arguments, if any, that should be used when making the {@link Rescorer}. This is additional
   *  information from the request that may be necessary to its logic, like current location. What it means
   *  is up to the implementation.
   * @return {@link PairRescorer} to use with {@link ServerRecommender#mostSimilarItems(String[], int, PairRescorer)}
   *  or {@code null} if none should be used. The {@link PairRescorer} will be passed, to its
   *  {@link PairRescorer#isFiltered(String, String)} method, the candidate item ID
   *  and item ID passed in the user query as its second element.
   *  Each non-filtered pair is passed with its original score to
   *  {@link PairRescorer#rescore(String, String, double)}
   */
  PairRescorer getMostSimilarItemsRescorer(OryxRecommender recommender, String... args);

}
