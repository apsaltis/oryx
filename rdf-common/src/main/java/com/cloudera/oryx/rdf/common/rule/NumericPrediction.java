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

import com.google.common.base.Preconditions;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;

import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.example.NumericFeature;

/**
 * Represents a predicted value of a numeric target. The prediction is simply a real number in this case.
 *
 * @author Sean Owen
 * @see CategoricalPrediction
 */
public final class NumericPrediction extends Prediction {

  private float prediction;

  public NumericPrediction(float prediction, int initialCount) {
    super(initialCount);
    this.prediction = prediction;
  }

  public float getPrediction() {
    return prediction;
  }

  @Override
  public FeatureType getFeatureType() {
    return FeatureType.NUMERIC;
  }

  @Override
  public synchronized void update(Example train) {
    NumericFeature target = (NumericFeature) train.getTarget();
    int oldCount = getCount();
    int newCount = oldCount + 1;
    setCount(newCount);
    prediction =
        (float) ((oldCount / (double) newCount) * prediction +
                 target.getValue() / (double) newCount);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NumericPrediction)) {
      return false;
    }
    NumericPrediction other = (NumericPrediction) o;
    return prediction == other.prediction;
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(prediction);
  }

  @Override
  public String toString() {
    return Float.toString(prediction);
  }

  static NumericPrediction buildNumericPrediction(Iterable<Example> examples) {
    StorelessUnivariateStatistic mean = new Mean();
    for (Example example : examples) {
      mean.increment(((NumericFeature) example.getTarget()).getValue());
    }
    Preconditions.checkState(mean.getN() > 0);
    return new NumericPrediction((float) mean.getResult(), (int) mean.getN());
  }

}
