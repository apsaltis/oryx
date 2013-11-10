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

package com.cloudera.oryx.rdf.common.rule;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.FeatureType;

/**
 * Tests {@link CategoricalPrediction}.
 *
 * @author Sean Owen
 */
public final class CategoricalPredictionTest extends OryxTest {

  @Test
  public void testConstruct() {
    int[] counts = { 0, 1, 3, 0, 4, 0};
    CategoricalPrediction prediction = new CategoricalPrediction(counts);
    assertEquals(FeatureType.CATEGORICAL, prediction.getFeatureType());
    assertEquals(4, prediction.getMostProbableCategoryID());
    assertArrayEquals(counts, prediction.getCategoryCounts());
    assertArrayEquals(new float[] {0.0f, 0.125f, 0.375f, 0.0f, 0.5f, 0.0f}, prediction.getCategoryProbabilities());
  }

  @Test
  public void testConstructFromProbability() {
    float[] probability = {0.0f, 0.125f, 0.375f, 0.0f, 0.5f, 0.0f};
    CategoricalPrediction prediction = new CategoricalPrediction(probability);
    assertEquals(FeatureType.CATEGORICAL, prediction.getFeatureType());
    assertEquals(4, prediction.getMostProbableCategoryID());
    assertNull(prediction.getCategoryCounts());
    assertArrayEquals(probability, prediction.getCategoryProbabilities());
  }

  @Test
  public void testUpdate() {
    int[] counts = { 0, 1, 3, 0, 4, 0};
    CategoricalPrediction prediction = new CategoricalPrediction(counts);
    Example example = new Example(CategoricalFeature.forValue(2));
    prediction.update(example);
    prediction.update(example);
    assertEquals(2, prediction.getMostProbableCategoryID());
    counts[2] += 2;
    assertArrayEquals(counts, prediction.getCategoryCounts());
    assertArrayEquals(new float[] {0.0f, 0.1f, 0.5f, 0.0f, 0.4f, 0.0f}, prediction.getCategoryProbabilities());
  }

}
