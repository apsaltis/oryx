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

package com.cloudera.oryx.als.computation;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.OryxRecommender;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.io.DelimitedDataUtils;

/**
 * Runs a mixed, concurrent load against a given recommender instance.
 * 
 * @author Sean Owen
 */
public final class LoadRunner implements Callable<Object> {
  
  private static final Logger log = LoggerFactory.getLogger(LoadRunner.class);
  
  private final OryxRecommender client;
  private final String[] uniqueUserIDs;
  private final String[] uniqueItemIDs;
  private final int steps;

  /**
   * @param client recommender to load
   * @param dataDirectory a directory containing data files from which user and item IDs should be read
   * @param steps number of load steps to run
   */
  public LoadRunner(OryxRecommender client, File dataDirectory, int steps) throws IOException {
    Preconditions.checkNotNull(client);
    Preconditions.checkNotNull(dataDirectory);
    Preconditions.checkArgument(steps > 0);  
    
    log.info("Reading IDs...");    
    Set<String> userIDsSet = Sets.newHashSet();
    Set<String> itemIDsSet = Sets.newHashSet();
    for (File f : dataDirectory.listFiles(IOUtils.CSV_COMPRESSED_FILTER)) {
      if (!f.getName().contains("oryx-append")) {
        for (CharSequence line : new FileLineIterable(f)) {
          String[] columns = DelimitedDataUtils.decode(line);
          userIDsSet.add(columns[0]);
          itemIDsSet.add(columns[1]);
        }
      }
    }
    
    this.client = client;    
    this.uniqueUserIDs = userIDsSet.toArray(new String[userIDsSet.size()]);
    this.uniqueItemIDs = itemIDsSet.toArray(new String[itemIDsSet.size()]);
    this.steps = steps;
  }

  public int getSteps() {
    return steps;
  }

  @Override
  public Void call() throws Exception {
    runLoad();
    return null;
  }
  
  public void runLoad() throws InterruptedException {

    final StorelessUnivariateStatistic recommendedBecause = new Mean();
    final StorelessUnivariateStatistic setPreference = new Mean();
    final StorelessUnivariateStatistic removePreference = new Mean();
    final StorelessUnivariateStatistic ingest = new Mean();
    final StorelessUnivariateStatistic refresh = new Mean();
    final StorelessUnivariateStatistic estimatePreference = new Mean();
    final StorelessUnivariateStatistic mostSimilarItems = new Mean();
    final StorelessUnivariateStatistic similarityToItem = new Mean();
    final StorelessUnivariateStatistic mostPopularItems = new Mean();
    final StorelessUnivariateStatistic recommendToMany = new Mean();
    final StorelessUnivariateStatistic recommend = new Mean();
    final RandomGenerator random = RandomManager.getRandom();

    int numCores = Runtime.getRuntime().availableProcessors();
    final int stepsPerWorker = steps / numCores;
    Collection<Callable<Object>> workers = Lists.newArrayListWithCapacity(numCores);
    for (int i = 0; i < numCores; i++) {
      workers.add(new Callable<Object>() {
        @Override
        public Void call() throws Exception {
          for (int i = 0; i < stepsPerWorker; i++) {
            double r;
            String userID;
            String itemID;
            String itemID2;
            float value;
            synchronized (random) {
              r = random.nextDouble();
              userID = uniqueUserIDs[random.nextInt(uniqueUserIDs.length)];
              itemID = uniqueItemIDs[random.nextInt(uniqueItemIDs.length)];
              itemID2 = uniqueItemIDs[random.nextInt(uniqueItemIDs.length)];
              value = random.nextInt(10);
            }
            long stepStart = System.currentTimeMillis();
            if (r < 0.05) {
              client.recommendedBecause(userID, itemID, 10);
              recommendedBecause.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.07) {
              client.setPreference(userID, itemID);
              setPreference.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.08) {
              client.setPreference(userID, itemID, value);
              setPreference.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.11) {
              client.removePreference(userID, itemID);
              removePreference.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.12) {
              Reader reader = new StringReader(DelimitedDataUtils.encode(userID, itemID, Float.toString(value)) + '\n');
              client.ingest(reader);
              ingest.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.13) {
              client.refresh();
              refresh.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.14) {
              client.similarityToItem(itemID, itemID2);
              similarityToItem.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.15) {
              client.mostPopularItems(10);
              mostPopularItems.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.19) {
              client.estimatePreference(userID, itemID);
              estimatePreference.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.20) {
              client.estimateForAnonymous(itemID, new String[]{itemID2});
              estimatePreference.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.25) {
              client.mostSimilarItems(new String[]{itemID}, 10);
              mostSimilarItems.increment(System.currentTimeMillis() - stepStart);
            } else if (r < 0.30) {
              client.recommendToMany(new String[]{userID, userID}, 10, true, null);
              recommendToMany.increment(System.currentTimeMillis() - stepStart);
            } else {
              client.recommend(userID, 10);
              recommend.increment(System.currentTimeMillis() - stepStart);
            }
          }
          return null;
        }
      });
    }

    log.info("Starting load test...");

    long start = System.currentTimeMillis();
    ExecutorService executor = Executors.newFixedThreadPool(numCores);
    Iterable<Future<Object>> futures;
    try {
      futures = executor.invokeAll(workers);
    } finally {
      ExecutorUtils.shutdownNowAndAwait(executor);
    }
    long end = System.currentTimeMillis();

    ExecutorUtils.checkExceptions(futures);

    log.info("Finished {} steps in {}ms", steps, end - start);

    log.info("recommendedBecause: {}", recommendedBecause.getResult());
    log.info("setPreference: {}", setPreference.getResult());
    log.info("removePreference: {}", removePreference.getResult());
    log.info("ingest: {}", ingest.getResult());
    log.info("refresh: {}", refresh.getResult());
    log.info("estimatePreference: {}", estimatePreference.getResult());
    log.info("mostSimilarItems: {}", mostSimilarItems.getResult());
    log.info("similarityToItem: {}", similarityToItem.getResult());
    log.info("mostPopularItems: {}", mostPopularItems.getResult());        
    log.info("recommendToMany: {}", recommendToMany.getResult());
    log.info("recommend: {}", recommend.getResult());    
  }

}
