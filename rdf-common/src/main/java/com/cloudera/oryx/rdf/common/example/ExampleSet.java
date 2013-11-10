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

package com.cloudera.oryx.rdf.common.example;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.math3.util.FastMath;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.cloudera.oryx.rdf.common.rule.Decision;

/**
 * Encapsulates a set of {@link Example}s, including derived information about the data set, such
 * as the type of each feature and the type of the target.
 *
 * @author Sean Owen
 */
public final class ExampleSet implements Iterable<Example> {

  private final List<Example> examples;
  private final FeatureType[] featureTypes;
  private final int[] categoryCounts;
  private final FeatureType targetType;
  private final int targetCategoryCount;

  public ExampleSet(List<Example> examples) {
    Preconditions.checkNotNull(examples);
    Preconditions.checkArgument(!examples.isEmpty());
    this.examples = examples;

    Example first = examples.get(0);
    int numFeatures = first.getNumFeatures();
    featureTypes = new FeatureType[numFeatures];
    for (Example example : examples) {
      boolean allNonNull = true;
      for (int i = 0; i < numFeatures; i++) {
        if (featureTypes[i] == null) {
          allNonNull = false;
          Feature feature = example.getFeature(i);
          if (feature != null) {
            featureTypes[i] = feature.getFeatureType();
          }
        }
      }
      if (allNonNull) {
        break;
      }
    }
    targetType = first.getTarget().getFeatureType();

    categoryCounts = new int[numFeatures];
    int theTargetCategoryCount = 0;

    for (Example example : examples) {
      for (int i = 0; i < numFeatures; i++) {
        if (featureTypes[i] == FeatureType.CATEGORICAL) {
          CategoricalFeature feature = (CategoricalFeature) example.getFeature(i);
          if (feature != null) {
            categoryCounts[i] = FastMath.max(categoryCounts[i], feature.getValueID() + 1);
          }
        }
      }
      if (targetType == FeatureType.CATEGORICAL) {
        theTargetCategoryCount = FastMath.max(theTargetCategoryCount,
                                              ((CategoricalFeature) example.getTarget()).getValueID() + 1);
      }
    }
    this.targetCategoryCount = theTargetCategoryCount;
  }

  private ExampleSet(List<Example> subset, ExampleSet of) {
    this.examples = subset;
    this.featureTypes = of.featureTypes;
    this.categoryCounts = of.categoryCounts;
    this.targetType = of.targetType;
    this.targetCategoryCount = of.targetCategoryCount;
  }

  public List<Example> getExamples() {
    return examples;
  }

  public int getNumFeatures() {
    return featureTypes.length;
  }

  public FeatureType getFeatureType(int featureNumber) {
    return featureTypes[featureNumber];
  }

  public int getCategoryCount(int featureNumber) {
    return categoryCounts[featureNumber];
  }

  public FeatureType getTargetType() {
    return targetType;
  }

  public int getTargetCategoryCount() {
    return targetCategoryCount;
  }

  @Override
  public Iterator<Example> iterator() {
    return examples.iterator();
  }

  public ExampleSet subset(List<Example> explicitSubset) {
    return new ExampleSet(explicitSubset, this);
  }

  public ExampleSet[] split(Decision decision) {
    List<Example> positive = Lists.newArrayList();
    List<Example> negative = Lists.newArrayList();
    for (Example example : examples) {
      if (decision.isPositive(example)) {
        positive.add(example);
      } else {
        negative.add(example);
      }
    }
    return new ExampleSet[] { subset(negative), subset(positive) };
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(Arrays.toString(featureTypes)).append(" -> ").append(targetType).append('\n');
    for (Example example : examples) {
      result.append(example).append('\n');
    }
    return result.toString();
  }

}
