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

import java.util.List;

import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.FeatureType;

/**
 * Subclasses represent a binary yes/no positive/negative decision based on the value of a feature in
 * an example. This can be a numeric feature or a categorical feature.
 *
 * @author Sean Owen
 * @see NumericDecision
 * @see CategoricalDecision
 */
public abstract class Decision {

  private final int featureNumber;
  private double informationGain;

  Decision(int featureNumber) {
    this.featureNumber = featureNumber;
    this.informationGain = Double.NaN;
  }

  /**
   * @return number of the feature whose value the {@code Decision} operates on
   */
  public final int getFeatureNumber() {
    return featureNumber;
  }

  /**
   * @return information gain that the {@code Decision} gives rise to (on the training set used to build the model);
   *  this is not set until the {@code Decision} is evaluated
   */
  public final double getInformationGain() {
    return informationGain;
  }

  public final void setInformationGain(double informationGain) {
    this.informationGain = informationGain;
  }

  /**
   * @param example example to evaluate {@code Decision} on
   * @return true iff the {@code Decision} is positive on this example
   */
  public abstract boolean isPositive(Example example);

  /**
   * @return default decision -- true means positive -- when the {@code Decision} cannot be evaluated
   *  on an {@link Example} because the feature value it decides on is missing
   */
  public abstract boolean getDefaultDecision();

  /**
   * @return type of feature the {@code Decision} is over (a {@link FeatureType})
   */
  public abstract FeatureType getType();

  /**
   * @param examples {@link ExampleSet} of data to create {@code Decision}s over
   * @param featureNumber which feature to decide on
   * @param suggestedMaxSplitCandidates a suggested maximum number of {@code Decision}s to return
   * @return a {@link List} of {@code Decision}s to evaluate
   */
  public static List<Decision> decisionsFromExamples(ExampleSet examples,
                                                     int featureNumber,
                                                     int suggestedMaxSplitCandidates) {
    switch (examples.getFeatureType(featureNumber)) {
      case NUMERIC:
        return NumericDecision.numericDecisionsFromExamples(featureNumber, examples, suggestedMaxSplitCandidates);
      case CATEGORICAL:
        return CategoricalDecision.categoricalDecisionsFromExamples(featureNumber,
                                                                    examples,
                                                                    suggestedMaxSplitCandidates);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();
  
  @Override
  public abstract String toString();
  
}
