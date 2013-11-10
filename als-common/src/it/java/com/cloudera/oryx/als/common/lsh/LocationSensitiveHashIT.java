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

package com.cloudera.oryx.als.common.lsh;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.math.SimpleVectorMath;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.random.RandomUtils;

/**
 * A long-runnign integration test for {@link LocationSensitiveHash}.
 *
 * @author Sean Owen
 */
public final class LocationSensitiveHashIT extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(LocationSensitiveHashIT.class);

  private static final int NUM_FEATURES = 30;
  private static final int NUM_ITEMS = 1000000;
  private static final int NUM_RECS = 10;
  private static final int ITERATIONS = 10;
  private static final double LN2 = Math.log(2.0);

  @Test
  public void testLSH() {
    RandomGenerator random = RandomManager.getRandom();

    Mean avgPercentTopRecsConsidered = new Mean();
    Mean avgNDCG = new Mean();
    Mean avgPercentAllItemsConsidered = new Mean();

    for (int iteration = 0; iteration < ITERATIONS; iteration++) {

      LongObjectMap<float[]> Y = new LongObjectMap<float[]>();
      for (int i = 0; i < NUM_ITEMS; i++) {
        Y.put(i, RandomUtils.randomUnitVector(NUM_FEATURES, random));
      }
      float[] userVec = RandomUtils.randomUnitVector(NUM_FEATURES, random);

      double[] results = doTestRandomVecs(Y, userVec);
      double percentTopRecsConsidered = results[0];
      double ndcg = results[1];
      double percentAllItemsConsidered = results[2];

      log.info("Considered {}% of all candidates, {} nDCG, got {}% recommendations correct",
               100 * percentAllItemsConsidered,
               ndcg,
               100 * percentTopRecsConsidered);

      avgPercentTopRecsConsidered.increment(percentTopRecsConsidered);
      avgNDCG.increment(ndcg);
      avgPercentAllItemsConsidered.increment(percentAllItemsConsidered);
    }

    log.info("{}", avgPercentTopRecsConsidered.getResult());
    log.info("{}", avgNDCG.getResult());
    log.info("{}", avgPercentAllItemsConsidered.getResult());

    assertTrue(avgPercentTopRecsConsidered.getResult() > 0.8);
    assertTrue(avgNDCG.getResult() > 0.8);
    assertTrue(avgPercentAllItemsConsidered.getResult() < 0.09);
  }

  private static double[] doTestRandomVecs(LongObjectMap<float[]> Y, float[] userVec) {

    LocationSensitiveHash lsh = new LocationSensitiveHash(Y, 0.1, 20);

    LongSet candidates = new LongSet();
    float[][] userVecs = { userVec };
    for (Iterator<LongObjectMap.MapEntry<float[]>> candidatesIterator : lsh.getCandidateIterator(userVecs)) {
      while (candidatesIterator.hasNext()) {
        candidates.add(candidatesIterator.next().getKey());
      }
    }

    List<Long> topIDs = findTopRecommendations(Y, userVec);

    double score = 0.0;
    double maxScore = 0.0;
    int intersectionSize = 0;
    for (int i = 0; i < topIDs.size(); i++) {
      double value = LN2 / Math.log(2.0 + i);
      long id = topIDs.get(i);
      if (candidates.contains(id)) {
        intersectionSize++;
        score += value;
      }
      maxScore += value;
    }

    double percentTopRecsConsidered = (double) intersectionSize / topIDs.size();
    double ndcg = maxScore == 0.0 ? 0.0 : score / maxScore;
    double percentAllItemsConsidered = (double) candidates.size() / Y.size();

    return new double[] {percentTopRecsConsidered, ndcg, percentAllItemsConsidered};
  }

  private static List<Long> findTopRecommendations(LongObjectMap<float[]> Y, float[] userVec) {
    SortedMap<Double,Long> allScores = Maps.newTreeMap(Collections.reverseOrder());
    for (LongObjectMap.MapEntry<float[]> entry : Y.entrySet()) {
      double dot = SimpleVectorMath.dot(entry.getValue(), userVec);
      allScores.put(dot, entry.getKey());
    }
    List<Long> topRecommendations = Lists.newArrayList();
    for (Map.Entry<Double,Long> entry : allScores.entrySet()) {
      topRecommendations.add(entry.getValue());
      if (topRecommendations.size() == NUM_RECS) {
        return topRecommendations;
      }
    }
    return topRecommendations;
  }

}
