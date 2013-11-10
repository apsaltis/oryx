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
import com.google.common.collect.Lists;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.cloudera.oryx.common.collection.BitSet;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.information.Information;

/**
 * Represents a decision over a categorical feature. If the categorical feature takes on values from the
 * set V = { v1, v2, ... } then the rule is defined by a subset of V. The decision evaluates as positive if
 * an example's value for this feature is in the rule's subset.
 *
 * @author Sean Owen
 * @see NumericDecision
 */
public final class CategoricalDecision extends Decision {
  
  private final BitSet activeCategories;
  private final boolean defaultDecision;

  public CategoricalDecision(int featureNumber, BitSet activeCategories, boolean defaultDecision) {
    super(featureNumber);
    this.activeCategories = activeCategories;
    this.defaultDecision = defaultDecision;
  }

  private CategoricalDecision(int featureNumber,
                              int categoryCount,
                              List<Pair<Double,Integer>> scoreCategoryPairs,
                              int from,
                              int to,
                              int maxCategory) {
    super(featureNumber);
    activeCategories = new BitSet(categoryCount);
    for (int i = from; i < to; i++) {
      activeCategories.set(scoreCategoryPairs.get(i).getSecond());
    }
    defaultDecision = activeCategories.get(maxCategory);
  }

  /**
   * @return category value IDs which are considered positive by this decision
   */
  public BitSet getCategoryIDs() {
    return activeCategories;
  }

  @Override
  public boolean getDefaultDecision() {
    return defaultDecision;
  }

  @Override
  public boolean isPositive(Example example) {
    CategoricalFeature feature = (CategoricalFeature) example.getFeature(getFeatureNumber());
    return feature == null ? defaultDecision : activeCategories.get(feature.getValueID());
  }

  @Override
  public FeatureType getType() {
    return FeatureType.CATEGORICAL;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CategoricalDecision)) {
      return false;
    }
    CategoricalDecision other = (CategoricalDecision) o;
    return getFeatureNumber() == other.getFeatureNumber() && activeCategories.equals(other.activeCategories);
  }

  @Override
  public int hashCode() {
    return getFeatureNumber() ^ activeCategories.hashCode();
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("(#").append(getFeatureNumber()).append(" ∈ [");
    int category = -1;
    boolean first = true;
    while ((category = activeCategories.nextSetBit(category + 1)) >= 0) {
      if (first) {
        first = false;
      } else {
        result.append(',');
      }
      result.append(category);
    }
    result.append("])");
    return result.toString();
  }
  
  static List<Decision> categoricalDecisionsFromExamples(int featureNumber,
                                                         ExampleSet examples,
                                                         int suggestedMaxSplitCandidates) {
    switch (examples.getTargetType()) {
      case NUMERIC:
        return categoricalDecisionsForNumericTarget(featureNumber, examples, suggestedMaxSplitCandidates);
      case CATEGORICAL:
        return categoricalDecisionsForCategoricalTarget(featureNumber, examples, suggestedMaxSplitCandidates);
      default:
        throw new IllegalStateException();
    }
  }
  
  private static List<Decision> categoricalDecisionsForNumericTarget(int featureNumber,
                                                                     ExampleSet examples,
                                                                     int suggestedMaxSplitCandidates) {
    // PLANET paper claims this is optimal:
    int categoryCount = examples.getCategoryCount(featureNumber);
    Mean[] averageTargetForCategory = new Mean[categoryCount];
    for (Example example : examples) {
      CategoricalFeature feature = (CategoricalFeature) example.getFeature(featureNumber);
      if (feature == null) {
        continue;
      }
      int category = feature.getValueID();
      Mean categoryAverage = averageTargetForCategory[category];
      if (categoryAverage == null) {
        categoryAverage = new Mean();
        averageTargetForCategory[category] = categoryAverage;
      }
      categoryAverage.increment(((NumericFeature) example.getTarget()).getValue());
    }

    int maxCategory = -1;
    int maxCount = -1;
    for (int i = 0; i < averageTargetForCategory.length; i++) {
      Mean average = averageTargetForCategory[i];
      if (average != null && average.getN() > maxCount) {
        maxCount = (int) averageTargetForCategory[i].getN();
        maxCategory = i;
      }
    }
    Preconditions.checkArgument(maxCategory >= 0);
    
    List<Pair<Double,Integer>> byScore = Lists.newArrayListWithCapacity(averageTargetForCategory.length);
    for (int featureCategory = 0; featureCategory < averageTargetForCategory.length; featureCategory++) {
      StorelessUnivariateStatistic mean = averageTargetForCategory[featureCategory];
      if (mean != null) {
        byScore.add(new Pair<Double,Integer>(mean.getResult(), featureCategory));
      }
    }
    return sortAndGetDecisionsOverSubset(featureNumber,
                                         categoryCount,
                                         byScore,
                                         maxCategory,
                                         suggestedMaxSplitCandidates);
  }

  private static List<Decision> categoricalDecisionsForCategoricalTarget(int featureNumber,
                                                                         ExampleSet examples,
                                                                         int suggestedMaxSplitCandidates) {
    int categoryCount = examples.getCategoryCount(featureNumber);
    int[] countsForFeature = new int[categoryCount];
    int[][] targetCountsForFeature = new int[categoryCount][];
    for (Example example : examples) {
      CategoricalFeature feature = (CategoricalFeature) example.getFeature(featureNumber);
      if (feature == null) {
        continue;
      }
      int featureCategory = feature.getValueID();
      countsForFeature[featureCategory]++;
      int[] targetCounts = targetCountsForFeature[featureCategory];
      if (targetCounts == null) {
        targetCounts = new int[examples.getTargetCategoryCount()];
        targetCountsForFeature[featureCategory] = targetCounts;
      }
      int targetCategory = ((CategoricalFeature) example.getTarget()).getValueID();
      targetCounts[targetCategory]++;
    }

    int maxCategory = -1;
    int maxCount = -1;
    for (int i = 0; i < countsForFeature.length; i++) {
      if (countsForFeature[i] > maxCount) {
        maxCount = countsForFeature[i];
        maxCategory = i;
      }
    }
    Preconditions.checkArgument(maxCategory >= 0);

    List<Pair<Double,Integer>> byScore = Lists.newArrayListWithCapacity(targetCountsForFeature.length);
    for (int featureCategory = 0; featureCategory < targetCountsForFeature.length; featureCategory++) {
      int[] targetCategoryCounts = targetCountsForFeature[featureCategory];
      if (targetCategoryCounts != null) {
        double entropy = Information.entropy(targetCategoryCounts);
        byScore.add(new Pair<Double,Integer>(entropy, featureCategory));
      }
    }

    return sortAndGetDecisionsOverSubset(featureNumber,
                                         categoryCount,
                                         byScore,
                                         maxCategory,
                                         suggestedMaxSplitCandidates);
  }

  private static List<Decision> sortAndGetDecisionsOverSubset(int featureNumber,
                                                              int categoryCount,
                                                              List<Pair<Double,Integer>> sorted,
                                                              int maxCategory,
                                                              int suggestedMaxSplitCandidates) {
    Collections.sort(sorted, PairComparator.INSTANCE);
    // The vital condition here is that if decision n decides an example is positive, then all subsequent
    // decisions in the list will also find it positive. Here we create decisions with sets of categories that
    // are always supersets of previous ones.
    int numDecisions = FastMath.min(sorted.size(), suggestedMaxSplitCandidates);
    // Going to take the decisions based on earlier splits, if we need to cut it down.
    // These are decisions that partition the data into a low-entropy subset, and everything else.
    // These are likely to be the best.
    List<Decision> decisions = Lists.newArrayListWithCapacity(numDecisions - 1);
    for (int i = 1; i < numDecisions; i++) {
      decisions.add(new CategoricalDecision(featureNumber, categoryCount, sorted, 0, i, maxCategory));
    }
    return decisions;
  }

  private static final class PairComparator implements Comparator<Pair<Double,?>>, Serializable {
    private static final Comparator<Pair<Double,?>> INSTANCE = new PairComparator();
    @Override
    public int compare(Pair<Double,?> a, Pair<Double,?> b) {
      return a.getFirst().compareTo(b.getFirst());
    }
  }

}