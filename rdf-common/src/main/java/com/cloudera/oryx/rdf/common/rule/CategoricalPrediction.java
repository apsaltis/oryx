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

import java.util.Arrays;

import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.FeatureType;

/**
 * Represents a prediction of the value of a categorical target. The prediction is not just a single category
 * value, but a probability distribution over all known category values.
 *
 * @author Sean Owen
 * @see NumericPrediction
 */
public final class CategoricalPrediction extends Prediction {

  private final int[] categoryCounts;
  private final float[] categoryProbabilities;
  private int maxCategory;

  public CategoricalPrediction(int[] categoryCounts) {
    super(sum(categoryCounts));
    this.categoryCounts = categoryCounts;
    this.categoryProbabilities = new float[categoryCounts.length];
    recompute();
  }

  private static int sum(int[] categoryCounts) {
    int total = 0;
    for (int count : categoryCounts) {
      total += count;
    }
    return total;
  }

  private void recompute() {
    int total = getCount();
    int maxCount = -1;
    int theMaxCategory = -1;
    for (int i = 0; i < categoryCounts.length; i++) {
      int count = categoryCounts[i];
      if (count > maxCount) {
        maxCount = count;
        theMaxCategory = i;
      }
      categoryProbabilities[i] = (float) count / total;
    }
    Preconditions.checkArgument(theMaxCategory >= 0);
    maxCategory = theMaxCategory;
  }

  public CategoricalPrediction(float[] categoryProbabilities) {
    super(0);
    this.categoryCounts = null;
    this.categoryProbabilities = categoryProbabilities;
    float maxProbability = Float.NEGATIVE_INFINITY;
    int theMaxCategory = -1;
    for (int i = 0; i < categoryProbabilities.length; i++) {
      float probability = categoryProbabilities[i];
      if (probability > maxProbability) {
        maxProbability = probability;
        theMaxCategory = i;
      }
    }
    Preconditions.checkArgument(theMaxCategory >= 0);
    maxCategory = theMaxCategory;
  }

  public int[] getCategoryCounts() {
    return categoryCounts;
  }

  public float[] getCategoryProbabilities() {
    return categoryProbabilities;
  }

  public int getMostProbableCategoryID() {
    return maxCategory;
  }

  /**
   * @return {@link FeatureType#CATEGORICAL}
   */
  @Override
  public FeatureType getFeatureType() {
    return FeatureType.CATEGORICAL;
  }

  @Override
  public synchronized void update(Example train) {
    CategoricalFeature target = (CategoricalFeature) train.getTarget();
    int valueID = target.getValueID();
    categoryCounts[valueID]++;
    setCount(getCount() + 1);
    recompute();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CategoricalPrediction)) {
      return false;
    }
    CategoricalPrediction other = (CategoricalPrediction) o;
    return Arrays.equals(categoryProbabilities, other.categoryProbabilities);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(categoryProbabilities);
  }

  @Override
  public String toString() {
    return ':' + Arrays.toString(categoryProbabilities);
  }

  static CategoricalPrediction buildCategoricalPrediction(ExampleSet examples) {
    int[] categoryCount = new int[examples.getTargetCategoryCount()];
    for (Example example : examples) {
      int category = ((CategoricalFeature) example.getTarget()).getValueID();
      categoryCount[category]++;
    }
    return new CategoricalPrediction(categoryCount);
  }

}
