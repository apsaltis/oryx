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

import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.util.FastMath;

import java.util.Collections;
import java.util.List;

import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.example.NumericFeature;

/**
 * Represents a decision over a numeric feature. Decisions are defined simply by a threshold value; the
 * decision is positive if the feature's value is greater than or equal to this decision's threshold value.
 *
 * @author Sean Owen
 * @see CategoricalDecision
 */
public final class NumericDecision extends Decision {

  private final float threshold;
  private final boolean defaultDecision;

  public NumericDecision(int featureNumber, float threshold, boolean defaultDecision) {
    super(featureNumber);
    this.threshold = threshold;
    this.defaultDecision = defaultDecision;
  }

  private NumericDecision(int featureNumber, float threshold, float mean) {
    super(featureNumber);
    this.threshold = threshold;
    this.defaultDecision = mean >= threshold;
  }

  /**
   * @return decision threshold; feature values greater than or equal are considered positive
   */
  public float getThreshold() {
    return threshold;
  }

  @Override
  public boolean getDefaultDecision() {
    return defaultDecision;
  }

  @Override
  public boolean isPositive(Example example) {
    NumericFeature feature = (NumericFeature) example.getFeature(getFeatureNumber());
    return feature == null ? defaultDecision : feature.getValue() >= threshold;
  }

  @Override
  public FeatureType getType() {
    return FeatureType.NUMERIC;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NumericDecision)) {
      return false;
    }
    NumericDecision other = (NumericDecision) o;
    return getFeatureNumber() == other.getFeatureNumber() && threshold == other.threshold;
  }

  @Override
  public int hashCode() {
    return getFeatureNumber() ^ Float.floatToIntBits(threshold);
  }

  @Override
  public String toString() {
    return "(#" + getFeatureNumber() + " >= " + threshold + ')';
  }

  static List<Decision> numericDecisionsFromExamples(int featureNumber,
                                                     Iterable<Example> examples,
                                                     int suggestedMaxSplitCandidates) {
    Multiset<Float> sortedFeatureValueCounts = TreeMultiset.create();
    StorelessUnivariateStatistic mean = new Mean();
    int numExamples = 0;
    for (Example example : examples) {
      NumericFeature feature = (NumericFeature) example.getFeature(featureNumber);
      if (feature == null) {
        continue;
      }
      numExamples++;
      float value = feature.getValue();
      sortedFeatureValueCounts.add(value, 1);
      mean.increment(value);
    }

    // Make decisions from split points that divide up input into roughly equal amounts of examples
    List<Decision> decisions = Lists.newArrayListWithExpectedSize(suggestedMaxSplitCandidates);
    int approxExamplesPerSplit = FastMath.max(1, numExamples / suggestedMaxSplitCandidates);
    int examplesInSplit = 0;
    float lastValue = Float.NaN;
    // This will iterate in order of value by nature of TreeMap
    for (Multiset.Entry<Float> entry : sortedFeatureValueCounts.entrySet()) {
      float value = entry.getElement();
      if (examplesInSplit >= approxExamplesPerSplit) {
        decisions.add(new NumericDecision(featureNumber, (value + lastValue) / 2.0f, (float) mean.getResult()));
        examplesInSplit = 0;
      }
      examplesInSplit += entry.getCount();
      lastValue = value;
    }

    // The vital condition here is that if decision n decides an example is positive, then all subsequent
    // decisions in the list will also find it positive. So we need to order from highest threshold to lowest
    Collections.reverse(decisions);
    return decisions;
  }

}
