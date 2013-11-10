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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.IDValue;
import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.common.Rescorer;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.serving.generation.ALSGenerationManager;
import com.cloudera.oryx.common.LangUtils;
import com.cloudera.oryx.als.common.NoSuchItemException;
import com.cloudera.oryx.als.common.NoSuchUserException;
import com.cloudera.oryx.als.common.NotReadyException;
import com.cloudera.oryx.als.common.OryxRecommender;
import com.cloudera.oryx.common.ReloadingReference;
import com.cloudera.oryx.als.common.PairRescorer;
import com.cloudera.oryx.als.common.TopN;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.als.serving.candidate.CandidateFilter;
import com.cloudera.oryx.common.math.SimpleVectorMath;
import com.cloudera.oryx.als.serving.generation.Generation;
import com.cloudera.oryx.common.io.DelimitedDataUtils;

/**
 * <p>The core implementation of {@link OryxRecommender} that lies inside the Serving Layer.</p>
 *
 * @author Sean Owen
 */
public final class ServerRecommender implements OryxRecommender, Closeable {
  
  private static final Logger log = LoggerFactory.getLogger(ServerRecommender.class);

  private final ALSGenerationManager generationManager;
  private final int numCores;
  private final ReloadingReference<ExecutorService> executor;

  public ServerRecommender(File localInputDir) throws IOException {
    Preconditions.checkNotNull(localInputDir, "No local dir");

    numCores = ExecutorUtils.getParallelism();
    executor = new ReloadingReference<ExecutorService>(new Callable<ExecutorService>() {
      @Override
      public ExecutorService call() {
        return Executors.newFixedThreadPool(
            2 * numCores,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ServerRecommender-%d").build());
      }
    });

    this.generationManager = new ALSGenerationManager(localInputDir);
    this.generationManager.refresh();
  }

  @Override
  public void refresh() {
    generationManager.refresh();
  }

  @Override
  public void ingest(File file) throws IOException {
    Reader reader = null;
    try {
      reader = IOUtils.openReaderMaybeDecompressing(file);
      ingest(reader);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Override
  public void ingest(Reader reader) {
    for (CharSequence line : new FileLineIterable(reader)) {
      String[] columns = DelimitedDataUtils.decode(line);
      String userID = columns[0];
      String itemID = columns[1];
      if (columns.length > 2) {
        String valueToken = columns[2];
        float value = valueToken.isEmpty() ? Float.NaN : LangUtils.parseFloat(valueToken);
        if (Float.isNaN(value)) {
          removePreference(userID, itemID);
        } else {
          setPreference(userID, itemID, value);
        }
      } else {
        setPreference(userID, itemID);
      }
    }
  }

  @Override
  public void close() {
    generationManager.close();
    ExecutorService executorService = executor.maybeGet();
    if (executorService != null) {
      ExecutorUtils.shutdownNowAndAwait(executorService);
    }
  }

  /**
   * @throws NotReadyException if {@link ALSGenerationManager#getCurrentGeneration()} returns null
   */
  private Generation getCurrentGeneration() throws NotReadyException {
    Generation generation = generationManager.getCurrentGeneration();
    if (generation == null) {
      throw new NotReadyException();
    }
    return generation;
  }

  /**
   * Like {@link #recommend(String, int, Rescorer)} but supplies no rescorer.
   */
  @Override
  public List<IDValue> recommend(String userID, int howMany) throws NoSuchUserException, NotReadyException {
    return recommend(userID, howMany, null);
  }

  /**
   * Like {@link #recommend(String, int, boolean, Rescorer)} and specifies to not consider known items.
   */
  @Override
  public List<IDValue> recommend(String userID, int howMany, Rescorer rescorer)
      throws NoSuchUserException, NotReadyException {
    return recommend(userID, howMany, false, rescorer);
  }

  /**
   * @param userID user for which recommendations are to be computed
   * @param howMany desired number of recommendations
   * @param considerKnownItems if true, items that the user is already associated to are candidates
   *  for recommendation. Normally this is {@code false}.
   * @param rescorer rescoring function used to modify association strengths before ranking results
   * @return {@link List} of recommended {@link IDValue}s, ordered from most strongly recommend to least
   * @throws NoSuchUserException if the user is not known in the model
   * @throws NotReadyException if the recommender has no model available yet
   */
  @Override
  public List<IDValue> recommend(String userID,
                                 int howMany,
                                 boolean considerKnownItems,
                                 Rescorer rescorer) throws NoSuchUserException, NotReadyException {
    return recommendToMany(new String[] { userID }, howMany,  considerKnownItems, rescorer);
  }

  @Override
  public List<IDValue> recommendToMany(String[] userIDs,
                                       int howMany,
                                       boolean considerKnownItems,
                                       Rescorer rescorer) throws NoSuchUserException, NotReadyException {

    Preconditions.checkArgument(howMany > 0, "howMany must be positive");

    Generation generation = getCurrentGeneration();
    LongObjectMap<float[]> X = generation.getX();

    Lock xLock = generation.getXLock().readLock();
    List<float[]> userFeatures = Lists.newArrayListWithCapacity(userIDs.length);
    xLock.lock();
    try {
      for (String userID : userIDs) {
        float[] theUserFeatures = X.get(StringLongMapping.toLong(userID));
        if (theUserFeatures != null) {
          userFeatures.add(theUserFeatures);
        }
      }
    } finally {
      xLock.unlock();
    }
    if (userFeatures.isEmpty()) {
      throw new NoSuchUserException(Arrays.toString(userIDs));
    }

    LongObjectMap<LongSet> knownItemIDs = generation.getKnownItemIDs();
    if (knownItemIDs == null && !considerKnownItems) {
      throw new UnsupportedOperationException("Can't ignore known items because no known items available");
    }
    LongSet usersKnownItemIDs = null;
    if (!considerKnownItems) {
      Lock knownItemLock = generation.getKnownItemLock().readLock();
      knownItemLock.lock();
      try {
        for (String userID : userIDs) {
          LongSet theKnownItemIDs = knownItemIDs.get(StringLongMapping.toLong(userID));
          if (theKnownItemIDs == null) {
            continue;
          }
          if (usersKnownItemIDs == null) {
            usersKnownItemIDs = theKnownItemIDs;
          } else {
            LongPrimitiveIterator it = usersKnownItemIDs.iterator();
            while (it.hasNext()) {
              if (!theKnownItemIDs.contains(it.nextLong())) {
                it.remove();
              }
            }
          }
          if (usersKnownItemIDs.isEmpty()) {
            break;
          }
        }
      } finally {
        knownItemLock.unlock();
      }
    }

    float[][] userFeaturesArray = userFeatures.toArray(new float[userFeatures.size()][]);
    Lock yLock = generation.getYLock().readLock();
    yLock.lock();
    try {
      return multithreadedTopN(userFeaturesArray,
                               usersKnownItemIDs,
                               rescorer,
                               howMany,
                               generation.getCandidateFilter());
    } finally {
      yLock.unlock();
    }

  }

  private List<IDValue> multithreadedTopN(final float[][] userFeatures,
                                          final LongSet userKnownItemIDs,
                                          final Rescorer rescorer,
                                          final int howMany,
                                          CandidateFilter candidateFilter) throws NotReadyException {

    Collection<Iterator<LongObjectMap.MapEntry<float[]>>> candidateIterators =
        candidateFilter.getCandidateIterator(userFeatures);

    int numIterators = candidateIterators.size();
    int parallelism = FastMath.min(numCores, numIterators);

    final Queue<NumericIDValue> topN = TopN.initialQueue(howMany);

    if (parallelism > 1) {

      ExecutorService executorService = executor.get();

      final Iterator<Iterator<LongObjectMap.MapEntry<float[]>>> candidateIteratorsIterator =
          candidateIterators.iterator();

      Collection<Future<Object>> futures = Lists.newArrayList();
      for (int i = 0; i < numCores; i++) {
        futures.add(executorService.submit(new Callable<Object>() {
          @Override
          public Void call() throws NotReadyException {
            float[] queueLeastValue = { Float.NEGATIVE_INFINITY };
            while (true) {
              Iterator<LongObjectMap.MapEntry<float[]>> candidateIterator;
              synchronized (candidateIteratorsIterator) {
                if (!candidateIteratorsIterator.hasNext()) {
                  break;
                }
                candidateIterator = candidateIteratorsIterator.next();
              }
              Iterator<NumericIDValue> partialIterator =
                  new RecommendIterator(userFeatures,
                                        candidateIterator,
                                        userKnownItemIDs,
                                        rescorer,
                                        getCurrentGeneration().getIDMapping());
              TopN.selectTopNIntoQueueMultithreaded(topN, queueLeastValue, partialIterator, howMany);
            }
            return null;
          }
        }));
      }
      ExecutorUtils.checkExceptions(futures);

    } else {

      for (Iterator<LongObjectMap.MapEntry<float[]>> candidateIterator : candidateIterators) {
        Iterator<NumericIDValue> partialIterator =
            new RecommendIterator(userFeatures,
                                  candidateIterator,
                                  userKnownItemIDs,
                                  rescorer,
                                  getCurrentGeneration().getIDMapping());
        TopN.selectTopNIntoQueue(topN, partialIterator, howMany);
      }

    }

    return translateToStringIDs(TopN.selectTopNFromQueue(topN, howMany));
  }

  private List<IDValue> translateToStringIDs(Collection<NumericIDValue> numericIDValues) throws NotReadyException {
    StringLongMapping mapping = getCurrentGeneration().getIDMapping();
    List<IDValue> translated = Lists.newArrayListWithCapacity(numericIDValues.size());
    for (NumericIDValue numericIDValue : numericIDValues) {
      translated.add(new IDValue(mapping.toString(numericIDValue.getID()), numericIDValue.getValue()));
    }
    return translated;
  }

  @Override
  public List<IDValue> recommendToAnonymous(String[] itemIDs, int howMany)
      throws NotReadyException, NoSuchItemException {
    return recommendToAnonymous(itemIDs, howMany, null);
  }

  @Override
  public List<IDValue> recommendToAnonymous(String[] itemIDs, float[] values, int howMany)
      throws NotReadyException, NoSuchItemException {
    return recommendToAnonymous(itemIDs, values, howMany, null);
  }

  @Override
  public List<IDValue> recommendToAnonymous(String[] itemIDs, int howMany, Rescorer rescorer)
      throws NotReadyException, NoSuchItemException {
    return recommendToAnonymous(itemIDs, null, howMany, rescorer);
  }

  @Override
  public List<IDValue> recommendToAnonymous(String[] itemIDs,
                                            float[] values,
                                            int howMany,
                                            Rescorer rescorer)
      throws NotReadyException, NoSuchItemException {

    Preconditions.checkArgument(howMany > 0, "howMany must be positive");

    float[] anonymousUserFeatures = buildAnonymousUserFeatures(itemIDs, values);

    LongSet userKnownItemIDs = new LongSet(itemIDs.length);
    for (String itemID : itemIDs) {
      userKnownItemIDs.add(StringLongMapping.toLong(itemID));
    }

    float[][] anonymousFeaturesAsArray = { anonymousUserFeatures };

    Generation generation = getCurrentGeneration();    
    Lock yLock = generation.getYLock().readLock();    
    yLock.lock();
    try {
      return multithreadedTopN(anonymousFeaturesAsArray,
                               userKnownItemIDs,
                               rescorer,
                               howMany,
                               generation.getCandidateFilter());
    } finally {
      yLock.unlock();
    }
  }
  
  private float[] buildAnonymousUserFeatures(String[] itemIDs, float[] values)
      throws NotReadyException, NoSuchItemException {

    Preconditions.checkArgument(values == null || values.length == itemIDs.length,
                                "Number of values doesn't match number of items");
    
    Generation generation = getCurrentGeneration();

    LongObjectMap<float[]> Y = generation.getY();
    Solver ytySolver = generation.getYTYSolver();
    if (ytySolver == null) {
      throw new NotReadyException();
    }

    float[] anonymousUserFeatures = null;
    Lock yLock = generation.getYLock().readLock();

    boolean anyItemIDFound = false;
    for (int j = 0; j < itemIDs.length; j++) {
      String itemID = itemIDs[j];
      float[] itemFeatures;
      yLock.lock();
      try {
        itemFeatures = Y.get(StringLongMapping.toLong(itemID));
      } finally {
        yLock.unlock();
      }
      if (itemFeatures == null) {
        continue;
      }
      anyItemIDFound = true;
      double[] userFoldIn = ytySolver.solveFToD(itemFeatures);
      if (anonymousUserFeatures == null) {
        anonymousUserFeatures = new float[userFoldIn.length];
      }
      double signedFoldInWeight = foldInWeight(0.0, values == null ? 1.0f : values[j]);
      if (signedFoldInWeight != 0.0) {
        for (int i = 0; i < anonymousUserFeatures.length; i++) {
          anonymousUserFeatures[i] += (float) (signedFoldInWeight * userFoldIn[i]);
        }
      }
    }
    if (!anyItemIDFound) {
      throw new NoSuchItemException(Arrays.toString(itemIDs));
    }

    return anonymousUserFeatures;
  }

  @Override
  public List<IDValue> mostPopularItems(int howMany) throws NotReadyException {
    return mostPopularItems(howMany, null);
  }

  @Override
  public List<IDValue> mostPopularItems(int howMany, Rescorer rescorer) throws NotReadyException {

    Preconditions.checkArgument(howMany > 0, "howMany must be positive");

    Generation generation = getCurrentGeneration();
    LongObjectMap<LongSet> knownItemIDs = generation.getKnownItemIDs();
    if (knownItemIDs == null) {
      throw new UnsupportedOperationException();
    }

    LongFloatMap itemCounts = new LongFloatMap();
    Lock knownItemReadLock = generation.getKnownItemLock().readLock();
    knownItemReadLock.lock();
    try {
        Lock xReadLock = generation.getXLock().readLock();
        xReadLock.lock();
        try {
          
          for (LongObjectMap.MapEntry<LongSet> entry : generation.getKnownItemIDs().entrySet()) {
            LongSet itemIDs = entry.getValue();
            synchronized (itemIDs) {
              LongPrimitiveIterator it = itemIDs.iterator();
              while (it.hasNext()) {
                long itemID = it.nextLong();
                itemCounts.increment(itemID, 1.0f);
              }
            }
          }
          
        } finally {
          xReadLock.unlock();
        }
    } finally {
      knownItemReadLock.unlock();
    }

    return translateToStringIDs(
        TopN.selectTopN(new MostPopularItemsIterator(itemCounts.entrySet().iterator(),
                                                     rescorer,
                                                     generation.getIDMapping()),
                        howMany));
  }

  /**
   * @param userID user ID whose preference is to be estimated
   * @param itemID item ID to estimate preference for
   * @return an estimate of the strength of the association between the user and item. These values are the
   *  same as will be returned from {@link #recommend(String, int)}. They are opaque values and have no interpretation
   *  other than that larger means stronger. The values are typically in the range [0,1] but are not guaranteed
   *  to be so. Note that 0 will be returned if the user or item is not known in the data.
   * @throws NotReadyException if the recommender has no model available yet
   */
  @Override
  public float estimatePreference(String userID, String itemID) throws NotReadyException {
    return estimatePreferences(userID, itemID)[0];
  }

  @Override
  public float[] estimatePreferences(String userID, String... itemIDs) throws NotReadyException {
    
    Generation generation = getCurrentGeneration();
    LongObjectMap<float[]> X = generation.getX();
    
    float[] userFeatures;
    Lock xLock = generation.getXLock().readLock();
    xLock.lock();
    try {
      userFeatures = X.get(StringLongMapping.toLong(userID));
    } finally {
      xLock.unlock();
    }
    if (userFeatures == null) {
      return new float[itemIDs.length]; // All 0.0f
    }
    
    LongObjectMap<float[]> Y = generation.getY();

    Lock yLock = generation.getYLock().readLock();
    yLock.lock();
    try {
      float[] result = new float[itemIDs.length];
      for (int i = 0; i < itemIDs.length; i++) {
        String itemID = itemIDs[i];
        float[] itemFeatures = Y.get(StringLongMapping.toLong(itemID));
        if (itemFeatures != null) {
          float value = (float) SimpleVectorMath.dot(itemFeatures, userFeatures);
          Preconditions.checkState(Floats.isFinite(value), "Bad estimate");
          result[i] = value;
        } // else leave value at 0.0f
      }
      return result;
    } finally {
      yLock.unlock();
    }
  }
  
  @Override
  public float estimateForAnonymous(String toItemID, String[] itemIDs) throws NotReadyException, NoSuchItemException {
    return estimateForAnonymous(toItemID, itemIDs, null);
  }
  
  @Override
  public float estimateForAnonymous(String toItemID, String[] itemIDs, float[] values)
      throws NotReadyException, NoSuchItemException {

    Generation generation = getCurrentGeneration();    
    LongObjectMap<float[]> Y = generation.getY();
    Lock yLock = generation.getYLock().readLock();
    float[] toItemFeatures;    
    yLock.lock();
    try {
      toItemFeatures = Y.get(StringLongMapping.toLong(toItemID));
    } finally {
      yLock.unlock();
    }
    
    if (toItemFeatures == null) {
      throw new NoSuchItemException(toItemID);
    }
    
    float[] anonymousUserFeatures = buildAnonymousUserFeatures(itemIDs, values);    
    
    return (float) SimpleVectorMath.dot(anonymousUserFeatures, toItemFeatures);
  }

  /**
   * Calls {@link #setPreference(String, String, float)} with value 1.0.
   */
  @Override
  public void setPreference(String userID, String itemID) {
    setPreference(userID, itemID, 1.0f);
  }

  @Override
  public void setPreference(String userID, String itemID, float value) {

    // Record datum
    try {
      generationManager.append(userID, itemID, value);
    } catch (IOException ioe) {
      log.warn("Could not append datum; continuing", ioe);
    }

    Generation generation;
    try {
      generation = getCurrentGeneration();
    } catch (NotReadyException nre) {
      // Corner case -- no model ready so all we can do is record (above). Don't fail the request.
      return;
    }

    long longUserID = StringLongMapping.toLong(userID);
    long longItemID = StringLongMapping.toLong(itemID);

    float[] userFeatures = getFeatures(longUserID, generation.getX(), generation.getXLock());

    boolean newItem;
    Lock yReadLock = generation.getYLock().readLock();
    yReadLock.lock();
    try {
      newItem = generation.getY().get(longItemID) == null;
    } finally {
      yReadLock.unlock();
    }
    if (newItem) {
      generation.getCandidateFilter().addItem(itemID);
    }
    
    float[] itemFeatures = getFeatures(longItemID, generation.getY(), generation.getYLock());

    updateFeatures(userFeatures, itemFeatures, value, generation);

    LongObjectMap<LongSet> knownItemIDs = generation.getKnownItemIDs();
    if (knownItemIDs != null) {
      LongSet userKnownItemIDs;
      ReadWriteLock knownItemLock = generation.getKnownItemLock();
      Lock knownItemReadLock = knownItemLock.readLock();
      knownItemReadLock.lock();
      try {
        userKnownItemIDs = knownItemIDs.get(longUserID);
        if (userKnownItemIDs == null) {
          userKnownItemIDs = new LongSet();
          Lock knownItemWriteLock = knownItemLock.writeLock();
          knownItemReadLock.unlock();
          knownItemWriteLock.lock();
          try {
            knownItemIDs.put(longUserID, userKnownItemIDs);
          } finally {
            knownItemReadLock.lock();
            knownItemWriteLock.unlock();
          }
        }
      } finally {
        knownItemReadLock.unlock();
      }

      synchronized (userKnownItemIDs) {
        userKnownItemIDs.add(longItemID);
      }
    }
  }
  
  private static float[] getFeatures(long longID, LongObjectMap<float[]> matrix, ReadWriteLock lock) {
    float[] features;
    Lock readLock = lock.readLock();
    readLock.lock();
    try {
      features = matrix.get(longID);
      if (features == null) {
        int numFeatures = countFeatures(matrix);
        if (numFeatures > 0) {
          features = new float[numFeatures];
          Lock writeLock = lock.writeLock();
          readLock.unlock();
          writeLock.lock();
          try {
            matrix.put(longID, features);
          } finally {
            readLock.lock();
            writeLock.unlock();
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return features;
  }
  
  private static void updateFeatures(float[] userFeatures, float[] itemFeatures, float value, Generation generation) {
    if (userFeatures == null || itemFeatures == null) {
      return;
    }
    double signedFoldInWeight = foldInWeight(SimpleVectorMath.dot(userFeatures, itemFeatures), value);
    if (signedFoldInWeight == 0.0) {
      return;
    }
    // Here, we are using userFeatures, which is a row of X, as if it were a column of X'.
    // This is multiplied on the left by (X'*X)^-1. That's our left-inverse of X or at least the one
    // column we need. Which is what the new data point is multiplied on the left by. The result is a column;
    // we scale to complete the multiplication of the fold-in and add it in.
    Solver xtxSolver = generation.getXTXSolver();
    double[] itemFoldIn = xtxSolver == null ? null : xtxSolver.solveFToD(userFeatures);

    // Same, but reversed. Multiply itemFeatures, which is a row of Y, on the right by (Y'*Y)^-1.
    // This is the right-inverse for Y', or at least the row we need. Because of the symmetries we can use
    // the same method above to carry out the multiply; the result is conceptually a row vector.
    // The result is scaled and added in.
    Solver ytySolver = generation.getYTYSolver();
    double[] userFoldIn = ytySolver == null ? null : ytySolver.solveFToD(itemFeatures);

    if (itemFoldIn != null) {
      for (int i = 0; i < itemFeatures.length; i++) {
        double delta = signedFoldInWeight * itemFoldIn[i];
        Preconditions.checkState(Doubles.isFinite(delta));
        itemFeatures[i] += (float) delta;
      }
    }
    if (userFoldIn != null) {
      for (int i = 0; i < userFeatures.length; i++) {
        double delta = signedFoldInWeight * userFoldIn[i];
        Preconditions.checkState(Doubles.isFinite(delta));
        userFeatures[i] += (float) delta;
      }
    }
  }

  private static int countFeatures(LongObjectMap<float[]> M) {
    // assumes the read lock is held
    return M.isEmpty() ? 0 : M.entrySet().iterator().next().getValue().length;
  }

  /**
   * This function decides how much of a folded-in user or item vector should be added to a target item or user
   * vector, respectively, on a new action. The idea is that a positive value should push the current value towards
   * 1, but not further, and a negative value should push towards 0, but not further. How much to move should be
   * mostly proportional to the size of the value. 0 should move the result not at all; 2 ought to move twice as
   * much as 1, etc. This isn't quite possible but can be approximated by moving a fraction 1-1/(1+value) of the
   * distance towards 1, or 0.
   */
  private static double foldInWeight(double estimate, float value) {
    Preconditions.checkState(Doubles.isFinite(estimate));
    double signedFoldInWeight;
    if (value > 0.0f && estimate < 1.0) {
      double multiplier = 1.0 - FastMath.max(0.0, estimate);
      signedFoldInWeight = (1.0 - 1.0 / (1.0 + value)) * multiplier;
    } else if (value < 0.0f && estimate > 0.0) {
      double multiplier = -FastMath.min(1.0, estimate);
      signedFoldInWeight = (1.0 - 1.0 / (1.0 - value)) * multiplier;
    } else {
      signedFoldInWeight = 0.0;
    }
    return signedFoldInWeight;
  }

  @Override
  public void removePreference(String userID, String itemID) {

    // Record datum
    try {
      generationManager.remove(userID, itemID);
    } catch (IOException ioe) {
      log.warn("Could not append datum; continuing", ioe);
    }

    Generation generation;
    try {
      generation = getCurrentGeneration();
    } catch (NotReadyException nre) {
      // Corner case -- no model ready so all we can do is record (above). Don't fail the request.
      return;
    }

    long longUserID = StringLongMapping.toLong(userID);
    long longItemID = StringLongMapping.toLong(itemID);

    ReadWriteLock knownItemLock = generation.getKnownItemLock();

    boolean removeUser = false;
    LongObjectMap<LongSet> knownItemIDs = generation.getKnownItemIDs();
    if (knownItemIDs != null) {

      Lock knownItemReadLock = knownItemLock.readLock();
      LongSet userKnownItemIDs;
      knownItemReadLock.lock();
      try {
        userKnownItemIDs = knownItemIDs.get(longUserID);
      } finally {
        knownItemReadLock.unlock();
      }

      if (userKnownItemIDs == null) {
        // Doesn't exist? So ignore this request
        return;
      }

      synchronized (userKnownItemIDs) {
        if (!userKnownItemIDs.remove(longItemID)) {
          // Item unknown, so ignore this request
          return;
        }
        removeUser = userKnownItemIDs.isEmpty();
      }
    }

    // We can proceed with the request

    LongObjectMap<float[]> X = generation.getX();

    ReadWriteLock xLock = generation.getXLock();

    if (removeUser) {

      Lock knownItemWriteLock = knownItemLock.writeLock();
      knownItemWriteLock.lock();
      try {
        knownItemIDs.remove(longUserID);
      } finally {
        knownItemWriteLock.unlock();
      }

      Lock xWriteLock = xLock.writeLock();
      xWriteLock.lock();
      try {
        X.remove(longUserID);
      } finally {
        xWriteLock.unlock();
      }

    }

  }

  /**
   * One-argument version of {@link #mostSimilarItems(String[], int)}.
   */
  @Override
  public List<IDValue> mostSimilarItems(String itemID, int howMany)
      throws NoSuchItemException, NotReadyException {
    return mostSimilarItems(itemID, howMany, null);
  }

  /**
   * One-argument version of {@link #mostSimilarItems(String[], int, PairRescorer)}.
   */
  @Override
  public List<IDValue> mostSimilarItems(String itemID, int howMany, PairRescorer rescorer)
      throws NoSuchItemException, NotReadyException {

    Preconditions.checkArgument(howMany > 0, "howMany must be positive");

    long longItemID = StringLongMapping.toLong(itemID);

    Generation generation = getCurrentGeneration();
    LongObjectMap<float[]> Y = generation.getY();

    Lock yLock = generation.getYLock().readLock();
    yLock.lock();
    try {

      float[] itemFeatures = Y.get(longItemID);
      if (itemFeatures == null) {
        throw new NoSuchItemException(itemID);
      }

      return translateToStringIDs(
          TopN.selectTopN(new MostSimilarItemIterator(Y.entrySet().iterator(),
                                                      new long[]{longItemID},
                                                      new float[][]{itemFeatures},
                                                      rescorer,
                                                      generation.getIDMapping()),
                          howMany));
    } finally {
      yLock.unlock();
    }

  }

  /**
   * Like {@link #mostSimilarItems(String[], int, PairRescorer)} but uses no rescorer.
   */
  @Override
  public List<IDValue> mostSimilarItems(String[] itemIDs, int howMany)
      throws NoSuchItemException, NotReadyException {
    return mostSimilarItems(itemIDs, howMany, null);
  }

  /**
   * Computes items most similar to an item or items. The returned items have the highest average similarity
   * to the given items.
   *
   * @param itemIDs items for which most similar items are required
   * @param howMany maximum number of similar items to return; fewer may be returned
   * @param rescorer rescoring function used to modify item-item similarities before ranking results
   * @return {@link IDValue}s representing the top recommendations for the user, ordered by quality,
   *  descending. The score associated to it is an opaque value. Larger means more similar, but no further
   *  interpretation may necessarily be applied.
   * @throws NoSuchItemException if any of the items is not known in the model
   * @throws NotReadyException if the recommender has no model available yet
   */
  @Override
  public List<IDValue> mostSimilarItems(String[] itemIDs, int howMany, PairRescorer rescorer)
      throws NoSuchItemException, NotReadyException {

    Preconditions.checkArgument(howMany > 0, "howMany must be positive");

    long[] longItemIDs = new long[itemIDs.length];
    for (int i = 0; i < longItemIDs.length; i++) {
      longItemIDs[i] = StringLongMapping.toLong(itemIDs[i]);
    }

    Generation generation = getCurrentGeneration();
    LongObjectMap<float[]> Y = generation.getY();

    Lock yLock = generation.getYLock().readLock();
    yLock.lock();
    try {

      List<float[]> itemFeatures = Lists.newArrayListWithCapacity(itemIDs.length);
      for (long longItemID : longItemIDs) {
        float[] features = Y.get(longItemID);
        if (features != null) {
          itemFeatures.add(features);
        }
      }
      if (itemFeatures.isEmpty()) {
        throw new NoSuchItemException(Arrays.toString(itemIDs));
      }
      float[][] itemFeaturesArray = itemFeatures.toArray(new float[itemFeatures.size()][]);

      return translateToStringIDs(
          TopN.selectTopN(new MostSimilarItemIterator(Y.entrySet().iterator(),
                                                      longItemIDs,
                                                      itemFeaturesArray,
                                                      rescorer,
                                                      generation.getIDMapping()),
                          howMany));
    } finally {
      yLock.unlock();
    }
  }

  @Override
  public float[] similarityToItem(String toItemID, String... itemIDs) throws NotReadyException, NoSuchItemException {

    Generation generation = getCurrentGeneration();
    LongObjectMap<float[]> Y = generation.getY();

    float[] similarities = new float[itemIDs.length];
    Lock yLock = generation.getYLock().readLock();
    yLock.lock();
    try {

      float[] toFeatures = Y.get(StringLongMapping.toLong(toItemID));
      if (toFeatures == null) {
        throw new NoSuchItemException(toItemID);
      }
      double toFeaturesNorm = SimpleVectorMath.norm(toFeatures);

      boolean anyFound = false;
      for (int i = 0; i < similarities.length; i++) {
        float[] features = Y.get(StringLongMapping.toLong(itemIDs[i]));
        if (features == null) {
          similarities[i] = Float.NaN;
        } else {
          anyFound = true;
          double featuresNorm = SimpleVectorMath.norm(features);
          similarities[i] = (float) (SimpleVectorMath.dot(features, toFeatures) / (featuresNorm * toFeaturesNorm));
        }
      }
      if (!anyFound) {
        throw new NoSuchItemException(Arrays.toString(itemIDs));
      }

    } finally {
      yLock.unlock();
    }

    return similarities;
  }

  /**
   * <p>Lists the items that were most influential in recommending a given item to a given user. Exactly how this
   * is determined is left to the implementation, but, generally this will return items that the user prefers
   * and that are similar to the given item.</p>
   *
   * <p>These values by which the results are ordered are opaque values and have no interpretation
   * other than that larger means stronger.</p>
   *
   * @param userID ID of user who was recommended the item
   * @param itemID ID of item that was recommended
   * @param howMany maximum number of items to return
   * @return {@link List} of {@link IDValue}, ordered from most influential in recommended the given
   *  item to least
   * @throws NoSuchUserException if the user is not known in the model
   * @throws NoSuchItemException if the item is not known in the model
   * @throws NotReadyException if the recommender has no model available yet
   */
  @Override
  public List<IDValue> recommendedBecause(String userID, String itemID, int howMany)
      throws NoSuchUserException, NoSuchItemException, NotReadyException {

    Preconditions.checkArgument(howMany > 0, "howMany must be positive");

    Generation generation = getCurrentGeneration();
    LongObjectMap<LongSet> knownItemIDs = generation.getKnownItemIDs();
    if (knownItemIDs == null) {
      throw new UnsupportedOperationException("No known item IDs available");
    }

    Lock knownItemLock = generation.getKnownItemLock().readLock();
    LongSet userKnownItemIDs;
    knownItemLock.lock();
    try {
      userKnownItemIDs = knownItemIDs.get(StringLongMapping.toLong(userID));
    } finally {
      knownItemLock.unlock();
    }
    if (userKnownItemIDs == null) {
      throw new NoSuchUserException(userID);
    }

    LongObjectMap<float[]> Y = generation.getY();

    Lock yLock = generation.getYLock().readLock();
    yLock.lock();
    try {

      float[] features = Y.get(StringLongMapping.toLong(itemID));
      if (features == null) {
        throw new NoSuchItemException(itemID);
      }
      LongObjectMap<float[]> toFeatures;
      synchronized (userKnownItemIDs) {
        toFeatures = new LongObjectMap<float[]>(userKnownItemIDs.size());
        LongPrimitiveIterator it = userKnownItemIDs.iterator();
        while (it.hasNext()) {
          long fromItemID = it.nextLong();
          float[] fromFeatures = Y.get(fromItemID);
          toFeatures.put(fromItemID, fromFeatures);
        }
      }

      return translateToStringIDs(
          TopN.selectTopN(new RecommendedBecauseIterator(toFeatures.entrySet().iterator(),
                                                         features),
                          howMany));
    } finally {
      yLock.unlock();
    }
  }

  @Override
  public boolean isReady() {
    try {
      getCurrentGeneration();
      return true;
    } catch (NotReadyException ignored) {
      return false;
    }
  }

  @Override
  public void await() throws InterruptedException {
    while (!isReady()) {
      Thread.sleep(1000L);
    }
  }

}
