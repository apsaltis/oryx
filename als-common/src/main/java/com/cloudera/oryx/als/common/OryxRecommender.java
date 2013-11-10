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

package com.cloudera.oryx.als.common;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * Interface to the ALS-based recommender in Oryx. This interface represents the Java API exposed
 * by the implementation.
 *
 * @author Sean Owen
 */
public interface OryxRecommender {

  /**
   * @param userID user for which recommendations are to be computed
   * @param howMany desired number of recommendations
   * @return {@link List} of recommended {@link IDValue}s, ordered from most strongly recommend to least
   * @throws NoSuchUserException if the user is not known
   */
  List<IDValue> recommend(String userID, int howMany) throws NoSuchUserException, NotReadyException;

  /**
   * @param userID user for which recommendations are to be computed
   * @param howMany desired number of recommendations
   * @param rescorer rescoring function to apply before final list of recommendations is determined
   * @return {@link List} of recommended {@link IDValue}s, ordered from most strongly recommend to least
   * @throws NoSuchUserException if the user is not known
   */
  List<IDValue> recommend(String userID, int howMany, Rescorer rescorer)
      throws NoSuchUserException, NotReadyException;

  /**
   * @param userID user ID whose preference is to be estimated
   * @param itemID item ID to estimate preference for
   * @return an estimated preference, which may not be the total strength from the input. This is always an
   *  estimate built from the model alone. Returns 0 if user or item is not known.
   */
  float estimatePreference(String userID, String itemID) throws NotReadyException;

  /**
   * <p>Adds to a user-item preference, or association. This is called in response to some action that indicates
   * the user has a stronger association to an item, like a click or purchase. It is intended to be called many
   * times for a user and item, as more actions are observed that associate the two. That is, this calls
   * <em>adds to</em> rather than <em>sets</em> the association.</p>
   *
   * <p>To "undo" associations, call this method with negative values, or
   * see {@link #removePreference(String, String)}.</p>
   *
   * <p>Value is <strong>not</strong> a rating, but a strength indicator. It may be negative.
   * Its magnitude should correspond to the degree to which an observed event suggests an association between
   * a user and item. A value twice as big should correspond to an event that suggests twice as strong an
   * association.</p>
   *
   * <p>For example, a click on a video might result in a call with value 1.0. Watching half of the video
   * might result in another call adding value 3.0. Finishing the video, another 3.0. Liking or sharing the video,
   * an additional 10.0. Clicking away from a video within 10 seconds might result in a -3.0.</p>
   *
   * @param userID user involved in the new preference
   * @param itemID item involved
   * @param value strength value
   */
  void setPreference(String userID, String itemID, float value);

  /**
   * <p>This method will remove an item from the user's set of known items,
   * making it eligible for recommendation again. If the user has no more items, this method will remove
   * the user too, such that new calls to {@link #recommend(String, int)} for example
   * will fail with {@link NoSuchUserException}.</p>
   *
   * <p>It does not affect any user-item association strengths.</p>
   *
   * <p>Contrast with calling {@link #setPreference(String, String, float)} with a negative value,
   * which merely records a negative association between the user and item.</p>
   */
  void removePreference(String userID, String itemID);

  /**
   * @param itemID ID of item for which to find most similar other items
   * @param howMany desired number of most similar items to find
   * @return items most similar to the given item, ordered from most similar to least
   * @throws NoSuchItemException if the item is not known
   */
  List<IDValue> mostSimilarItems(String itemID, int howMany)
      throws NoSuchItemException, NotReadyException;

  /**
   * @param itemID ID of item for which to find most similar other items
   * @param howMany desired number of most similar items to find
   * @param rescorer {@link PairRescorer} which can adjust item-item similarity estimates used to determine
   *  most similar items. The longs that will be passed to the {@link PairRescorer} are
   *  the candidate item that might be returned in the result as its first element, and the 
   *  {@code itemID} argument here as its second element.
   * @return items most similar to the given item, ordered from most similar to least
   * @throws NoSuchItemException if the item is not known
   */
  List<IDValue> mostSimilarItems(String itemID, int howMany, PairRescorer rescorer)
      throws NoSuchItemException, NotReadyException;

  /**
   * @param itemIDs IDs of item for which to find most similar other items
   * @param howMany desired number of most similar items to find estimates used to determine most similar items
   * @return items most similar to the given items, ordered from most similar to least
   * @throws NoSuchItemException if <em>none</em> of {@code itemIDs}
   *  exist in the model. Otherwise, unknown items are ignored.
   */
  List<IDValue> mostSimilarItems(String[] itemIDs, int howMany)
      throws NoSuchItemException, NotReadyException;

  /**
   * @param itemIDs IDs of item for which to find most similar other items
   * @param howMany desired number of most similar items to find
   * @param rescorer {@link PairRescorer} which can adjust item-item similarity estimates used to determine
   *  most similar items. The longs that will be passed to the {@link PairRescorer} are
   *  the candidate item that might be returned in the result as its first element, and one of the 
   *  {@code itemID} arguments here as its second element.
   * @return items most similar to the given items, ordered from most similar to least
   * @throws NoSuchItemException if <em>none</em> of {@code itemIDs}
   *  exist in the model. Otherwise, unknown items are ignored.
   */
  List<IDValue> mostSimilarItems(String[] itemIDs,
                                 int howMany,
                                 PairRescorer rescorer)
      throws NoSuchItemException, NotReadyException;

  /**
   * Like {@code refresh(Collection)} from Mahout, but the need for the argument does not exist.
   * Triggers a rebuild of the object's internal state, particularly, the matrix model.
   */
  void refresh();

  /**
   * <p>Lists the items that were most influential in recommending a given item to a given user. Exactly how this
   * is determined is left to the implementation, but, generally this will return items that the user prefers
   * and that are similar to the given item.</p>
   *
   * <p>This returns a {@link List} of {@link IDValue} which is a little misleading since it's returning
   * recommend<strong>ing</strong> items, but, I thought it more natural to just reuse this class since it
   * encapsulates an item and value. The value here does not necessarily have a consistent interpretation or
   * expected range; it will be higher the more influential the item was in the recommendation.</p>
   *
   * @param userID ID of user who was recommended the item
   * @param itemID ID of item that was recommended
   * @param howMany maximum number of items to return
   * @return {@link List} of {@link IDValue}, ordered from most influential in recommended the
   *  given item to least
   */
  List<IDValue> recommendedBecause(String userID, String itemID, int howMany)
      throws NoSuchUserException, NoSuchItemException, NotReadyException;

  /**
   * @param userID user for which recommendations are to be computed
   * @param howMany desired number of recommendations
   * @param considerKnownItems if true, items that the user is already associated to are candidates
   *  for recommendation. Normally this is {@code false}.
   * @param rescorer rescoring function used to modify association strengths before ranking results
   * @return {@link List} of recommended {@link IDValue}s, ordered from most strongly recommend to least
   * @throws NotReadyException if the recommender has no model available yet
   */
  List<IDValue> recommend(String userID,
                          int howMany,
                          boolean considerKnownItems,
                          Rescorer rescorer) throws NoSuchUserException, NotReadyException;

  /**
   * Recommends to a group of users all at once. It takes into account their tastes equally and produces
   * one set of recommendations that is deemed most suitable to them as a group. It is otherwise identical
   * to {@link #recommend(String, int, boolean, Rescorer)}.
   *
   * @see #recommend(String, int, boolean, Rescorer)
   * @throws NoSuchUserException if <em>none</em> of {@code userIDs}
   *  exist in the model. Otherwise, unknown users are ignored.
   */
  List<IDValue> recommendToMany(String[] userIDs,
                                int howMany,
                                boolean considerKnownItems,
                                Rescorer rescorer) throws NoSuchUserException, NotReadyException;

  /**
   * Computes recommendations for a user that is not known to the model yet; instead, the user's
   * associated items are supplied to the method and it proceeds as if a user with these associated
   * items were in the model.
   * 
   * @param itemIDs item IDs that the anonymous user has interacted with
   * @param howMany how many recommendations to return
   * @see #recommend(String, int)
   * @throws NotReadyException if the implementation has no usable model yet
   * @throws NoSuchItemException if <em>none</em> of {@code itemIDs}
   *  exist in the model. Otherwise, unknown items are ignored.
   */
  List<IDValue> recommendToAnonymous(String[] itemIDs, int howMany)
      throws NotReadyException, NoSuchItemException;

  /**
   * Like {@link #recommendToAnonymous(String[], int)} but allows specifying values associated with items.
   *
   * @param itemIDs item IDs that the anonymous user has interacted with
   * @param values values associated with given {@code itemIDs}. If not null, must be as many values as
   *  there are item IDs
   * @param howMany how many recommendations to return
   * @see #estimateForAnonymous(String, String[], float[])
   */
  List<IDValue> recommendToAnonymous(String[] itemIDs, float[] values, int howMany)
      throws NotReadyException, NoSuchItemException;

  /**
   * Like {@link #recommendToAnonymous(String[], int)} but rescorer results like
   * {@link #recommend(String, int, boolean, Rescorer)}. All items are assumed to be equally important
   * to the anonymous users -- strength "1".
   *
   * @param itemIDs item IDs that the anonymous user has interacted with
   * @param howMany how many recommendations to return
   * @param rescorer rescoring function used to modify association strengths before ranking results
   * @see #recommendToAnonymous(String[], int)
   * @throws NoSuchItemException if <em>none</em> of {@code itemIDs}
   *  exist in the model. Otherwise, unknown items are ignored.
   */
  List<IDValue> recommendToAnonymous(String[] itemIDs, int howMany, Rescorer rescorer)
      throws NotReadyException, NoSuchItemException;

  /**
   * Like {@link #recommendToAnonymous(String[], int, Rescorer)} but lets caller specify strength scores associated
   * to each of the items.
   *
   * @param itemIDs item IDs that the anonymous user has interacted with
   * @param values values corresponding to {@code itemIDs}
   * @param howMany how many recommendations to return
   * @param rescorer rescoring function used to modify association strengths before ranking results
   * @see #recommendToAnonymous(String[], int)
   * @see #recommendToAnonymous(String[], int, Rescorer)
   * @throws NoSuchItemException if <em>none</em> of {@code itemIDs}
   *  exist in the model. Otherwise, unknown items are ignored.
   */
  List<IDValue> recommendToAnonymous(String[] itemIDs,
                                     float[] values,
                                     int howMany,
                                     Rescorer rescorer)
      throws NotReadyException, NoSuchItemException;

  /**
   * @param howMany how many items to return
   * @return most popular items, where popularity is measured by the number of users interacting with
   *  the item
   * @throws NotReadyException if the implementation has no usable model yet
   * @throws UnsupportedOperationException if known items for each user have been configured to not
   *  be loaded or recorded
   */
  List<IDValue> mostPopularItems(int howMany) throws NotReadyException;

  List<IDValue> mostPopularItems(int howMany, Rescorer rescorer) throws NotReadyException;

  /**
   * @param toItemID item to calculate similarity to
   * @param itemIDs items to calculate similarity from
   * @return similarity of each item to the given. The values are opaque; higher means more similar.
   * @throws NoSuchItemException if <em>none</em> of {@code itemIDs}
   *  exist in the model or if {@code toItemID} does not exist. Otherwise, unknown items are ignored.
   */
  float[] similarityToItem(String toItemID, String... itemIDs) throws NotReadyException, NoSuchItemException;

  /**
   * A bulk version of {@link #estimatePreference(String, String)}, suitable for computing many estimates
   * at once. The return values correspond, in order, to the item IDs provided, in order.
   *
   * @see #estimatePreference(String, String)
   */
  float[] estimatePreferences(String userID, String... itemIDs) throws NotReadyException;

  /**
   * A version of {@link #estimatePreference(String, String)} that, like
   * {@link #recommendToAnonymous(String[], float[], int)}, operates on "anonymous" users --
   * defined not by a previously known set of data, but data given in the request.
   * 
   * @param toItemID item for which the anonymous user's strength of interaction is to be estimated
   * @param itemIDs item IDs that the anonymous user has interacted with
   * @see #recommendToAnonymous(String[], float[], int)
   */
  float estimateForAnonymous(String toItemID, String[] itemIDs) throws NotReadyException, NoSuchItemException;
  
  /**
   * A version of {@link #estimatePreference(String, String)} that, like
   * {@link #recommendToAnonymous(String[], float[], int)}, operates on "anonymous" users --
   * defined not by a previously known set of data, but data given in the request.
   * 
   * @param toItemID item for which the anonymous user's strength of interaction is to be estimated
   * @param itemIDs item IDs that the anonymous user has interacted with
   * @param values values corresponding to {@code itemIDs}
   * @see #recommendToAnonymous(String[], float[], int)
   */
  float estimateForAnonymous(String toItemID, String[] itemIDs, float[] values)
      throws NotReadyException, NoSuchItemException;
  
  /**
   * Like {@link #ingest(File)}, but reads from a {@link Reader}.
   *
   * @param reader source of CSV data to ingest
   */
  void ingest(Reader reader) throws IOException;

  /**
   * "Uploads" a series of new associations to the recommender; this is like a bulk version of
   * {@link #setPreference(String, String, float)}. The input file should be in CSV format, where each
   * line is of the form {@code userID,itemID,value}. Note that the file may be compressed. If it is
   * make sure that its name reflects its compression -- gzip ending in ".gz", zip ending in ".zip"
   *
   * @param file CSV file to ingest, possibly compressed.
   */
  void ingest(File file) throws IOException;

  // Some overloads that make sense in the project's model:

  /**
   * Defaults to value 1.0.
   *
   * @see #setPreference(String, String, float)
   */
  void setPreference(String userID, String itemID);

  /**
   * @return true if and only if the instance is ready to make recommendations; may be false for example
   *  while the recommender is still building an initial model
   */
  boolean isReady();

  /**
   * Blocks indefinitely until {@link #isReady()} returns {@code true}.
   * 
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  void await() throws InterruptedException;

}
