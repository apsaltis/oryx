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

import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.rule.Decision;

/**
 * Contains utility methods for working with entropy and information, measured in nats.
 *
 * @author Sean Owen
 * @see NumericalInformation
 * @see CategoricalInformation
 */
public final class Information {

  private Information() {
  }

  /**
   * @param decisions list of possible {@link Decision}s to consider to split the examples in {@link ExampleSet}
   * @param examples {@link ExampleSet} of examples to evaluate {@link Decision}s over
   * @return the best {@link Decision} from {@code decisions} -- the one that maximizes information gain --
   *  along with the information gain it achievies, in nats
   */
  public static Pair<Decision,Double> bestGain(Iterable<Decision> decisions, ExampleSet examples) {
    switch (examples.getTargetType()) {
      case NUMERIC:
        return NumericalInformation.bestGain(decisions, examples);
      case CATEGORICAL:
        return CategoricalInformation.bestGain(decisions, examples);
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * @param counts counts of distinct categories in a sample
   * @return entropy, in nats, of those counts
   */
  public static double entropy(int[] counts) {
    // Entropy is really, over all counts:
    //  sum (-p_i * ln p_i)
    // where p_i = count_i / total
    // terms where count is 0 or 1 are 0, so can go away
    // Here we actually compute total as we go and account for it later, which avoids
    // a second loop and avoids some divisions.
    // This means we divide by total at the end to account for the missing denominator in the first p_i term,
    // and end up adding (subtracting negative) ln total at the end to account for ln p_i.
    double entropy = 0.0;
    int total = 0;
    for (int count : counts) {
      // if count = 0 or count = 1 then count*ln(count) = 0
      if (count > 1) {
        // This is in nats to match differential entropy -- base e, not 2
        entropy -= count * Math.log(count);
      }
      total += count;
    }
    return entropy / total + Math.log(total);
  }

}
