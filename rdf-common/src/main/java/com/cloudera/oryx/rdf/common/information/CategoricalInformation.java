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

import org.apache.commons.math3.util.Pair;

import java.util.List;

import com.cloudera.oryx.common.collection.BitSet;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.rule.Decision;

/**
 * Utility methods for computing information, measured in nats, over a categorical feature. This
 * includes information gain induced by a {@link Decision} over that feature. Here, information is
 * evaluated as Shannon entropy over the distribution of category values in a set of feature values.
 *
 * @author Sean Owen
 * @see NumericalInformation
 */
final class CategoricalInformation {
  
  private CategoricalInformation() {
  }

  static Pair<Decision,Double> bestGain(Iterable<Decision> decisions, ExampleSet examples) {
    int numCategories = examples.getTargetCategoryCount();
    int[] countsNegative = new int[numCategories];
    // Start with everything considered a negative example:
    List<Example> exampleList = examples.getExamples();
    int numExamples = exampleList.size();
    for (Example example : exampleList) {
      int category = ((CategoricalFeature) example.getTarget()).getValueID();
      countsNegative[category]++;
    }
    int numNegative = numExamples;

    double entropyAll = Information.entropy(countsNegative);

    BitSet notYetPositiveExamples = new BitSet(numExamples);
    notYetPositiveExamples.set(0, numExamples);
    Decision bestDecision = null;
    double bestGain = Double.NEGATIVE_INFINITY;
    int[] countsPositive = new int[numCategories];
    int numPositive = 0;

    for (Decision decision : decisions) {
      int nextNotYetPositive = -1;
      while ((nextNotYetPositive = notYetPositiveExamples.nextSetBit(nextNotYetPositive + 1)) >= 0) {
        Example example = exampleList.get(nextNotYetPositive);
        if (decision.isPositive(example)) {
          int category = ((CategoricalFeature) example.getTarget()).getValueID();
          countsNegative[category]--;
          numNegative--;
          countsPositive[category]++;
          numPositive++;
          notYetPositiveExamples.clear(nextNotYetPositive);
        }
      }
      double entropyNegative = Information.entropy(countsNegative);
      double entropyPositive = Information.entropy(countsPositive);
      double gain = entropyAll -
          (numNegative * entropyNegative + numPositive * entropyPositive) / (numNegative + numPositive);
      if (gain > bestGain) {
        bestGain = gain;
        bestDecision = decision;
      }
    }

    return bestDecision == null ? null : new Pair<Decision,Double>(bestDecision, bestGain);
  }

}
