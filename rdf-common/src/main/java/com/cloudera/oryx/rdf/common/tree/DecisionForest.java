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

package com.cloudera.oryx.rdf.common.tree;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import com.typesafe.config.Config;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.cloudera.oryx.common.iterator.ArrayIterator;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.rdf.common.eval.Evaluation;
import com.cloudera.oryx.rdf.common.eval.WeightedPrediction;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.rule.Prediction;

/**
 * An ensemble classifier based on many {@link DecisionTree}s.
 *
 * @author Sean Owen
 * @see DecisionTree
 */
public final class DecisionForest implements Iterable<DecisionTree>, TreeBasedClassifier {

  private static final Logger log = LoggerFactory.getLogger(DecisionForest.class);

  private final DecisionTree[] trees;
  private final double[] weights;
  private final double[] evaluations;

  public static DecisionForest fromExamplesWithDefault(List<Example> examples) {
    Config config = ConfigUtils.getDefaultConfig();
    int numTrees = config.getInt("model.num-trees");
    double fractionOfFeaturesToTry = config.getDouble("model.fraction-features-to-try");
    int minNodeSize = config.getInt("model.min-node-size");
    double minInfoGainNats = config.getDouble("model.min-info-gain-nats");
    int suggestedMaxSplitCandidates = config.getInt("model.max-split-candidates");
    int maxDepth = config.getInt("model.max-depth");
    double sampleRate = config.getDouble("model.sample-rate");
    ExampleSet exampleSet = new ExampleSet(examples);
    return new DecisionForest(numTrees,
                              fractionOfFeaturesToTry,
                              minNodeSize,
                              minInfoGainNats,
                              suggestedMaxSplitCandidates,
                              maxDepth,
                              sampleRate,
                              exampleSet);
  }

  public DecisionForest(final int numTrees,
                        double fractionOfFeaturesToTry,
                        final int minNodeSize,
                        final double minInfoGainNats,
                        final int suggestedMaxSplitCandidates,
                        final int maxDepth,
                        final double sampleRate,
                        final ExampleSet examples) {
    Preconditions.checkArgument(numTrees > 1);
    final int numFeatures = examples.getNumFeatures();
    Preconditions.checkArgument(fractionOfFeaturesToTry > 0.0 && fractionOfFeaturesToTry <= 1.0);
    final int featuresToTry = FastMath.max(1, (int) (fractionOfFeaturesToTry * numFeatures));
    Preconditions.checkArgument(numFeatures >= 1);
    Preconditions.checkArgument(minNodeSize >= 1);
    Preconditions.checkArgument(minInfoGainNats >= 0.0);
    Preconditions.checkArgument(suggestedMaxSplitCandidates >= 1);
    Preconditions.checkArgument(maxDepth >= 1);
    Preconditions.checkArgument(sampleRate > 0.0 && sampleRate <= 1.0);

    weights = new double[numTrees];
    Arrays.fill(weights, 1.0);
    evaluations = new double[numTrees];
    Arrays.fill(evaluations, Double.NaN);
    final int folds = FastMath.min(numTrees - 1, FastMath.max(1, (int) (sampleRate * numTrees)));

    trees = new DecisionTree[numTrees];

    ExecutorService executor = Executors.newFixedThreadPool(determineParallelism(trees.length));
    try {
      Collection<Future<Object>> futures = Lists.newArrayListWithCapacity(trees.length);
      for (int i = 0; i < numTrees; i++) {
        final int treeID = i;
        futures.add(executor.submit(new Callable<Object>() {
          @Override
          public Void call() throws Exception {
            Collection<Example> allExamples = examples.getExamples();
            int totalExamples = allExamples.size();
            int expectedTrainingSize = (int) (totalExamples * sampleRate);
            int expectedCVSize = totalExamples - expectedTrainingSize;
            List<Example> trainingExamples = Lists.newArrayListWithExpectedSize(expectedTrainingSize);
            List<Example> cvExamples = Lists.newArrayListWithExpectedSize(expectedCVSize);
            for (Example example : allExamples) {
              if (IntMath.mod(IntMath.mod(example.hashCode(), numTrees) - treeID, numTrees) < folds) {
                trainingExamples.add(example);
              } else {
                cvExamples.add(example);
              }
            }
            trees[treeID] = new DecisionTree(numFeatures,
                                             featuresToTry,
                                             minNodeSize,
                                             minInfoGainNats,
                                             suggestedMaxSplitCandidates,
                                             maxDepth,
                                             examples.subset(trainingExamples));
            log.info("Finished tree {}", treeID);
            double[] weightEval = Evaluation.evaluateToWeight(trees[treeID], examples.subset(cvExamples));
            weights[treeID] = weightEval[0];
            evaluations[treeID] = weightEval[1];
            log.info("Tree {} eval: {}", treeID, weightEval[1]);
            return null;
          }
        }));
      }
      ExecutorUtils.checkExceptions(futures);
    } finally {
      ExecutorUtils.shutdownNowAndAwait(executor);
    }
  }

  public DecisionForest(DecisionTree[] trees, double[] weights) {
    this.trees = trees;
    this.weights = weights;
    this.evaluations = new double[weights.length];
  }

  @Override
  public Iterator<DecisionTree> iterator() {
    return ArrayIterator.forArray(trees);
  }

  /**
   * @return {@link DecisionTree}s in the ensemble forest
   */
  public DecisionTree[] getTrees() {
    return trees;
  }

  public double[] getWeights() {
    return weights;
  }

  public double[] getEvaluations() {
    return evaluations;
  }
  
  @Override
  public Prediction classify(Example test) {
    return WeightedPrediction.voteOnFeature(
        Lists.transform(Arrays.asList(trees), new TreeToPredictionFunction(test)), weights);
  }

  public double[] importance(ExampleSet testSet) {
    int numFeatures = testSet.getNumFeatures();
    int[] totalCounts = new int[numFeatures];
    double[] totalWeightedGains = new double[numFeatures];
    for (DecisionTree tree : trees) {
      tree.importance(testSet, totalCounts, totalWeightedGains);
    }
    for (int i = 0; i < numFeatures; i++) {
      totalWeightedGains[i] /= totalCounts[i];
    }
    return totalWeightedGains;
  }

  @Override
  public void update(Example train) {
    for (TreeBasedClassifier tree : trees) {
      tree.update(train);
    }
  }

  private static int determineParallelism(int numTrees) {
    int numCores = ExecutorUtils.getParallelism();
    if (numCores >= numTrees) {
      return numTrees;
    }
    // Try to round up threads so trees is a multiple of it
    int numThreads = numCores;
    while (numTrees % numThreads != 0 && numThreads < 2 * numCores) {
      numThreads++;
    }
    return numThreads;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (DecisionTree tree : trees) {
      result.append(tree).append('\n');
    }
    return result.toString();
  }

  private static final class TreeToPredictionFunction implements Function<DecisionTree, Prediction> {
    private final Example test;
    TreeToPredictionFunction(Example test) {
      this.test = test;
    }
    @Override
    public Prediction apply(DecisionTree tree) {
      return tree.classify(test);
    }
  }
}
