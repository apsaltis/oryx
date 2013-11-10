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

package com.cloudera.oryx.rdf.common.eval;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.rule.CategoricalPrediction;
import com.cloudera.oryx.rdf.common.rule.NumericPrediction;

/**
 * Tests {@link WeightedPrediction}.
 *
 * @author Sean Owen
 */
public final class WeightedPredictionTest extends OryxTest {

  @Test
  public void testNumericVote() {
    List<NumericPrediction> predictions = Lists.newArrayList(
        new NumericPrediction(1.0f, 1),
        new NumericPrediction(3.0f, 2),
        new NumericPrediction(6.0f, 3)
    );
    double[] weights = {1.0, 1.0, 1.0};
    NumericPrediction vote = (NumericPrediction) WeightedPrediction.voteOnFeature(predictions, weights);
    assertEquals(FeatureType.NUMERIC, vote.getFeatureType());
    assertEquals(10.0f/3.0f, vote.getPrediction());
  }

  @Test
  public void testNumericVoteWeighted() {
    List<NumericPrediction> predictions = Lists.newArrayList(
        new NumericPrediction(1.0f, 1),
        new NumericPrediction(3.0f, 2),
        new NumericPrediction(6.0f, 3)
    );
    double[] weights = {3.0, 2.0, 1.0};
    NumericPrediction vote = (NumericPrediction) WeightedPrediction.voteOnFeature(predictions, weights);
    assertEquals(FeatureType.NUMERIC, vote.getFeatureType());
    assertEquals(15.0f/6.0f, vote.getPrediction());
  }

  @Test
  public void testCategoricalVote() {
    List<CategoricalPrediction> predictions = Lists.newArrayList(
        new CategoricalPrediction(new int[] {0, 1, 2}),
        new CategoricalPrediction(new int[] {6, 2, 0}),
        new CategoricalPrediction(new int[] {0, 2, 0})
    );
    double[] weights = {1.0, 1.0, 1.0};
    CategoricalPrediction vote = (CategoricalPrediction) WeightedPrediction.voteOnFeature(predictions, weights);
    assertEquals(FeatureType.CATEGORICAL, vote.getFeatureType());
    assertEquals(1, vote.getMostProbableCategoryID());
  }

  @Test
  public void testCategoricalVoteWeighted() {
    List<CategoricalPrediction> predictions = Lists.newArrayList(
        new CategoricalPrediction(new int[] {0, 1, 2}),
        new CategoricalPrediction(new int[] {6, 2, 0}),
        new CategoricalPrediction(new int[] {0, 2, 0})
    );
    double[] weights = {1.0, 10.0, 1.0};
    CategoricalPrediction vote = (CategoricalPrediction) WeightedPrediction.voteOnFeature(predictions, weights);
    assertEquals(FeatureType.CATEGORICAL, vote.getFeatureType());
    assertEquals(0, vote.getMostProbableCategoryID());
  }

}
