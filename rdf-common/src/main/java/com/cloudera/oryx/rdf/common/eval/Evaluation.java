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

package com.cloudera.oryx.rdf.common.eval;

import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.util.FastMath;

import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.rule.CategoricalPrediction;
import com.cloudera.oryx.rdf.common.rule.NumericPrediction;
import com.cloudera.oryx.rdf.common.tree.TreeBasedClassifier;

/**
 * A utility class containing methods related to evaluating a decision forest classifier.
 *
 * @author Sean Owen
 */
public final class Evaluation {
  
  private Evaluation() {
  }

  /**
   * @param classifier a {@link com.cloudera.oryx.rdf.common.tree.TreeBasedClassifier} (e.g. {@link com.cloudera.oryx.rdf.common.tree.DecisionForest})
   *  trained on data with a numeric target
   * @param testSet test set to evaluate on
   * @return root mean squared error over the test set square root of mean squared difference between actual
   *  and predicted numeric target value
   */
  public static double rootMeanSquaredError(TreeBasedClassifier classifier, Iterable<Example> testSet) {
    StorelessUnivariateStatistic mse = new Mean();
    for (Example test : testSet) {
      NumericFeature actual = (NumericFeature) test.getTarget();
      NumericPrediction prediction = (NumericPrediction) classifier.classify(test);
      double diff = actual.getValue() - prediction.getPrediction();
      mse.increment(diff * diff);
    }
    return FastMath.sqrt(mse.getResult());
  }

  /**
   * @param testSet test set to evaluate on
   * @return average absolute value of numeric target value in the test set
   */
  private static double meanAbs(Iterable<Example> testSet) {
    StorelessUnivariateStatistic mean = new Mean();
    for (Example test : testSet) {
      NumericFeature actual = (NumericFeature) test.getTarget();
      mean.increment(FastMath.abs(actual.getValue()));
    }
    return mean.getResult();
  }

  /**
   * @param classifier a {@link com.cloudera.oryx.rdf.common.tree.TreeBasedClassifier} (e.g. {@link com.cloudera.oryx.rdf.common.tree.DecisionForest})
   *  trained on data with a numeric target
   * @param testSet test set to evaluate on
   * @return fractin of test examples whose categorical target value was correctly predicted by the classifier
   */
  public static double correctlyClassifiedFraction(TreeBasedClassifier classifier, Iterable<Example> testSet) {
    int correct = 0;
    int total = 0;
    for (Example test : testSet) {
      CategoricalFeature actual = (CategoricalFeature) test.getTarget();
      CategoricalPrediction prediction = (CategoricalPrediction) classifier.classify(test);
      if (actual.getValueID() == prediction.getMostProbableCategoryID()) {
        correct++;
      }
      total++;
    }
    return (double) correct / total;
  }

  /*
  public static double[] correctlyClassifiedFractionWithoutFeature(Classifier classifier, ExampleSet testSet) {
    int numFeatures = testSet.getNumFeatures();
    double[] correctFraction = new double[numFeatures];
    for (int omitFeature = 0; omitFeature < numFeatures; omitFeature++) {
      int correct = 0;
      int total = 0;
      RandomGenerator random = RandomManager.getRandom();
      List<Example> examples = testSet.getExamples();
      for (Example test : testSet.getExamples()) {
        Example randomOtherExample = examples.get(random.nextInt(examples.size()));
        Feature[] copiedFeatures = new Feature[test.getNumFeatures()];
        for (int i = 0; i < copiedFeatures.length; i++) {
          copiedFeatures[i] = i == omitFeature ? randomOtherExample.getFeature(i) : test.getFeature(i);
        }
        CategoricalFeature actual = (CategoricalFeature) test.getTarget();
        CategoricalPrediction prediction =
            (CategoricalPrediction) classifier.classify(new Example(actual, copiedFeatures));
        if (actual.getValueID() == prediction.getMostProbableCategoryID()) {
          correct++;
        }
        total++;
      }
      correctFraction[omitFeature] = (double) correct / total;
    }
    return correctFraction;
  }
   */

  /**
   * @param classifier a {@link com.cloudera.oryx.rdf.common.tree.TreeBasedClassifier} (e.g. {@link com.cloudera.oryx.rdf.common.tree.DecisionForest}) whose
   *  weights are to be determined from performance on a test set
   * @param testSet test set to evaluate on
   * @return a {@code double[]} containing two values. The first is a weight for the classifier; the meaning depends
   *  on the type of target variable, but will be higher when the classifier is more accurate. The second is a raw
   *  evaluation score, which again varies by type of target feature: RMSE for numeric targets, correctly classified
   *  fraction for categorical.
   */
  public static double[] evaluateToWeight(TreeBasedClassifier classifier, ExampleSet testSet) {
    switch (testSet.getTargetType()) {
      case NUMERIC:
        return numericTargetWeight(classifier, testSet);
      case CATEGORICAL:
        return categoricalTargetWeight(classifier, testSet);
      default:
        throw new IllegalStateException();
    }
  }
  
  private static double[] numericTargetWeight(TreeBasedClassifier classifier, Iterable<Example> testSet) {
    double rmse = rootMeanSquaredError(classifier, testSet);
    double aSmallBit = 0.0000001 * FastMath.max(1.0, meanAbs(testSet));
    return new double[] { 1.0 / (aSmallBit + rmse), rmse };
  }

  private static double[] categoricalTargetWeight(TreeBasedClassifier classifier, Iterable<Example> testSet) {
    double correct = correctlyClassifiedFraction(classifier, testSet);
    return new double[] { correct, correct };
  }
  
}
