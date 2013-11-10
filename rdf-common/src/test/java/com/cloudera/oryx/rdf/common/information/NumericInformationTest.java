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
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.util.FastMath;
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
 * Tests {@link NumericalInformation}.
 *
 * @author Sean Owen
 */
public final class NumericInformationTest extends OryxTest {

  @Test
  public void testInformationCategoricalFeature() {
    ExampleSet exampleSet = examplesForValuesForCategories(new float[][]{
        new float[]{1.0f, 1.5f},
        new float[]{5.5f, 7.0f},
        new float[]{2.0f, 5.0f},
    });
    List<Decision> decisions = Decision.decisionsFromExamples(exampleSet, 0, 100);
    assertEquals(2, decisions.size());
    BitSet categories0 = ((CategoricalDecision) decisions.get(0)).getCategoryIDs();
    BitSet categories1 = ((CategoricalDecision) decisions.get(1)).getCategoryIDs();
    assertEquals(1, categories0.cardinality());
    assertTrue(categories0.get(0));
    assertEquals(2, categories1.cardinality());
    assertTrue(categories1.get(0));
    assertTrue(categories1.get(2));

    Pair<Decision,Double> best = NumericalInformation.bestGain(decisions, exampleSet);
    assertEquals(categories0, ((CategoricalDecision) best.getFirst()).getCategoryIDs());

    StandardDeviation all = new StandardDeviation();
    all.incrementAll(new double[] {1.0,1.5,5.5,7.0,2.0,5.0});
    StandardDeviation positive = new StandardDeviation();
    positive.incrementAll(new double[] {1.0,1.5});
    StandardDeviation negative = new StandardDeviation();
    negative.incrementAll(new double[] {5.5,7.0,2.0,5.0});

    assertEquals(differentialEntropy(all)
                     - (2.0/6.0)*differentialEntropy(positive)
                     - (4.0/6.0)*differentialEntropy(negative),
                 best.getValue().doubleValue());
  }

  private static double differentialEntropy(StandardDeviation stdev) {
    return FastMath.log(stdev.getResult() * FastMath.sqrt(2.0 * FastMath.PI * FastMath.E));
  }

  private static ExampleSet examplesForValuesForCategories(float[][] valuesForCategories) {
    List<Example> examples = Lists.newArrayList();
    for (int category = 0; category < valuesForCategories.length; category++) {
      for (float value : valuesForCategories[category]) {
        examples.add(new Example(NumericFeature.forValue(value), CategoricalFeature.forValue(category)));
      }
    }
    return new ExampleSet(examples);
  }

  @Test
  public void testInformationNumericFeature() {
    ExampleSet exampleSet = examplesForFeaturesValues(
        new float[]{0.0f, 1.0f, 2.0f, 4.0f},
        new float[]{1.0f, 1.5f, 2.0f, 5.0f}
    );
    List<Decision> decisions = Decision.decisionsFromExamples(exampleSet, 0, 100);
    assertEquals(3, decisions.size());
    assertEquals(3.0f, ((NumericDecision) decisions.get(0)).getThreshold());
    assertEquals(1.5f, ((NumericDecision) decisions.get(1)).getThreshold());
    assertEquals(0.5f, ((NumericDecision) decisions.get(2)).getThreshold());
    Pair<Decision,Double> best = NumericalInformation.bestGain(decisions, exampleSet);
    assertEquals(1.5f, ((NumericDecision) best.getFirst()).getThreshold());

    StandardDeviation all = new StandardDeviation();
    all.incrementAll(new double[] {1.0,1.5,2.0,5.0});
    StandardDeviation positive = new StandardDeviation();
    positive.incrementAll(new double[] {1.0,1.5});
    StandardDeviation negative = new StandardDeviation();
    negative.incrementAll(new double[] {2.0,5.0});

    assertEquals(differentialEntropy(all)
                     - (2.0/4.0)*differentialEntropy(positive)
                     - (2.0/4.0)*differentialEntropy(negative),
                 best.getValue().doubleValue());
  }

  private static ExampleSet examplesForFeaturesValues(float[] features, float[] values) {
    List<Example> examples = Lists.newArrayList();
    for (int i = 0; i < features.length; i++) {
      examples.add(new Example(NumericFeature.forValue(values[i]), NumericFeature.forValue(features[i])));
    }
    return new ExampleSet(examples);
  }

}
