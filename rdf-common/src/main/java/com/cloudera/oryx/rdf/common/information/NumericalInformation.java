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
import com.cloudera.oryx.common.stats.UpdatableVariance;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.rule.Decision;

/**
 * <p>Utility methods for computing information, measured in nats, over a numeric feature. This
 * includes information gain induced by a {@link Decision} over that feature. Here, information
 * is measured by evaluating <a href="http://en.wikipedia.org/wiki/Differential_entropy">differential entropy</a>
 * over a set of values of that numeric feature. The decision tree model supposes that the values are normally
 * distributed around the mean, so differential entropy of ln(stdev * sqrt(2*pi*e)) is used.</p>
 *
 * <p>It would be more correct, I believe, to use
 * <a href="http://en.wikipedia.org/wiki/Limiting_density_of_discrete_points">relative entropy</a>.
 * This is computed as something like -integral(p log p/m)  =  -integral(p log p - p log m)  =
 * -integral(p log p) + integral(p log m). m(x) here is a constant and equal to the range of the values
 * in the set, which is a constant. The first term is differential entropy, and the second is log(m) * integral(p),
 * which is log(m) since the integral is 1. This is just a constant too, log(m). This will end up falling
 * out too so has no impact on which split is chosen to have the best gain.</p>
 *
 * @author Sean Owen
 * @see CategoricalInformation
 */
final class NumericalInformation {
  
  //private static final double HALF_LN_2_PI_E = 0.5 * Math.log(2.0 * Math.PI * Math.E);

  private NumericalInformation() {
  }

  static Pair<Decision,Double> bestGain(Iterable<Decision> decisions, ExampleSet examples) {

    UpdatableVariance varianceNegativeStat = new UpdatableVariance();

    // Start with everything considered a negative example:
    for (Example example : examples) {
      float value = ((NumericFeature) example.getTarget()).getValue();
      varianceNegativeStat.increment(value);
    }

    // Save this off
    double varianceAll = varianceNegativeStat.getResult();
    if (Double.isNaN(varianceAll) || varianceAll <= 0.0) {
      // Weird case, no information at all
      return null;
    }
    // Entropy in nats is ln (stdev * sqrt(2*pi*e)) = ln(stdev) + 0.5*ln(2*pi*e) = 0.5*ln(variance) + ...
    // Actually to compute gain, we only need to know the ln(variance) since the additive constants will
    // fall out when we take the difference in entropies.
    //double entropyAll = 0.5 * Math.log(varianceAll) + HALF_LN_2_PI_E;
    double logVarianceAll = Math.log(varianceAll);

    List<Example> exampleList = examples.getExamples();
    BitSet notYetPositiveExamples = new BitSet(exampleList.size());
    notYetPositiveExamples.set(0, exampleList.size());
    Decision bestDecision = null;
    double bestGain = Double.NEGATIVE_INFINITY;
    UpdatableVariance variancePositiveStat = new UpdatableVariance();

    for (Decision decision : decisions) {
      boolean noChange = true;
      int nextNotYetPositive = -1;
      while ((nextNotYetPositive = notYetPositiveExamples.nextSetBit(nextNotYetPositive + 1)) >= 0) {
        Example example = exampleList.get(nextNotYetPositive);
        if (decision.isPositive(example)) {
          noChange = false;
          float value = ((NumericFeature) example.getTarget()).getValue();
          varianceNegativeStat.decrement(value);
          variancePositiveStat.increment(value);
          notYetPositiveExamples.clear(nextNotYetPositive);
        }
      }
      if (noChange) {
        continue;
      }

      double variancePositive = variancePositiveStat.getResult();
      double varianceNegative = varianceNegativeStat.getResult();
      if (Double.isNaN(variancePositive) || variancePositive <= 0.0 ||
          Double.isNaN(varianceNegative) || varianceNegative <= 0.0) {
        continue;
      }

      //double entropyNegative = 0.5 * Math.log(varianceNegative) + HALF_LN_2_PI_E;
      //double entropyPositive = 0.5 * Math.log(variancePositive) + HALF_LN_2_PI_E;
      double logVarianceNegative = Math.log(varianceNegative);
      double logVariancePositive = Math.log(variancePositive);
      long numNegative = varianceNegativeStat.getN();
      long numPositive = variancePositiveStat.getN();
      //double oldgain = entropyAll -
      //    (numNegative * entropyNegative + numPositive * entropyPositive) / (numNegative + numPositive);
      double gain =
          0.5 * (logVarianceAll - (numNegative * logVarianceNegative + numPositive * logVariancePositive) /
                                  (numNegative + numPositive));
      if (gain > bestGain) {
        bestGain = gain;
        bestDecision = decision;
      }
    }

    return bestDecision == null ? null : new Pair<Decision,Double>(bestDecision, bestGain);
  }

}
