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

package com.cloudera.oryx.rdf.common.information;

import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;

import java.util.List;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.BitSet;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.rule.CategoricalDecision;
import com.cloudera.oryx.rdf.common.rule.Decision;
import com.cloudera.oryx.rdf.common.rule.NumericDecision;

/**
 * Tests {@link CategoricalInformation}.
 *
 * @author Sean Owen
 */
public final class CategoricalInformationTest extends OryxTest {

  @Test
  public void testInformationCategoricalFeature() {
    ExampleSet exampleSet = examplesForCategoryCounts(new int[][] {
        {2, 6},
        {1, 1},
        {0, 1},
    });
    List<Decision> decisions = Decision.decisionsFromExamples(exampleSet, 0, 100);
    assertEquals(2, decisions.size());
    BitSet categories0 = ((CategoricalDecision) decisions.get(0)).getCategoryIDs();
    BitSet categories1 = ((CategoricalDecision) decisions.get(1)).getCategoryIDs();
    assertEquals(1, categories0.cardinality());
    assertTrue(categories0.get(2));
    assertEquals(2, categories1.cardinality());
    assertTrue(categories1.get(0));
    assertTrue(categories1.get(2));

    Pair<Decision,Double> best = CategoricalInformation.bestGain(decisions, exampleSet);
    assertEquals(categories0, ((CategoricalDecision) best.getFirst()).getCategoryIDs());
    assertEquals(Information.entropy(new int[] {3,8})
                     - (10.0/11.0)*Information.entropy(new int[] {3,7})
                     - ( 1.0/11.0)*Information.entropy(new int[] {0,1}),
                 best.getValue().doubleValue());
  }

  private static ExampleSet examplesForCategoryCounts(int[][] categoryCountsByCatFeatureValue) {
    List<Example> examples = Lists.newArrayList();
    for (int featureValue = 0; featureValue < categoryCountsByCatFeatureValue.length; featureValue++) {
      int[] categoryCounts = categoryCountsByCatFeatureValue[featureValue];
      for (int category = 0; category < categoryCounts.length; category++) {
        int count = categoryCounts[category];
        for (int i = 0; i < count; i++) {
          examples.add(new Example(CategoricalFeature.forValue(category), CategoricalFeature.forValue(featureValue)));
        }
      }
    }
    return new ExampleSet(examples);
  }

  @Test
  public void testInformationNumericFeature() {
    ExampleSet exampleSet = examplesForNumericValues(new float[][]{
        new float[]{1.0f, 2.0f},
        new float[]{0.0f, 3.0f},
    });
    List<Decision> decisions = Decision.decisionsFromExamples(exampleSet, 0, 100);
    assertEquals(3, decisions.size());
    assertEquals(2.5f, ((NumericDecision) decisions.get(0)).getThreshold());
    assertEquals(1.5f, ((NumericDecision) decisions.get(1)).getThreshold());
    assertEquals(0.5f, ((NumericDecision) decisions.get(2)).getThreshold());
    Pair<Decision,Double> best = CategoricalInformation.bestGain(decisions, exampleSet);
    assertEquals(2.5f, ((NumericDecision) best.getFirst()).getThreshold());
    assertEquals(Information.entropy(new int[] {2,2})
                     - (1.0/4.0)*Information.entropy(new int[] {0,1})
                     - (3.0/4.0)*Information.entropy(new int[] {2,1}),
                 best.getValue().doubleValue());
  }

  private static ExampleSet examplesForNumericValues(float[][] valuesForCategoryValue) {
    List<Example> examples = Lists.newArrayList();
    for (int category = 0; category < valuesForCategoryValue.length; category++) {
      for (float value : valuesForCategoryValue[category]) {
        examples.add(new Example(CategoricalFeature.forValue(category), NumericFeature.forValue(value)));
      }
    }
    return new ExampleSet(examples);
  }

}
