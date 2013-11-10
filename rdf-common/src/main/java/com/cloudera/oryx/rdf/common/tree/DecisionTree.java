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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.Pair;

import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.information.Information;
import com.cloudera.oryx.rdf.common.rule.Decision;
import com.cloudera.oryx.rdf.common.rule.Prediction;

/**
 * A decision-tree classifier. Given a set of training {@link Example}s, builds a model by randomly choosing
 * subsets of features and the training set, and then finding a binary {@link Decision} over those features and data
 * that produces the largest information gain in the two subsets it implies. This is repeated to build a tree
 * of {@link DecisionNode}s. At the bottom, leaf nodes are formed ({@link TerminalNode}) that contain a
 * {@link Prediction} of the target value.
 *
 * @author Sean Owen
 * @see DecisionForest
 */
public final class DecisionTree implements TreeBasedClassifier {

  private final TreeNode root;

  public static DecisionTree fromExamplesWithDefault(List<Example> examples) {
    ExampleSet exampleSet = new ExampleSet(examples);
    Config config = ConfigUtils.getDefaultConfig();
    int numFeatures = exampleSet.getNumFeatures();
    double fractionOfFeaturesToTry = config.getDouble("model.fraction-features-to-try");
    int featuresToTry = FastMath.max(1, (int) (fractionOfFeaturesToTry * numFeatures));
    int minNodeSize = config.getInt("model.min-node-size");
    double minInfoGainNats = config.getDouble("model.min-info-gain-nats");
    int suggestedMaxSplitCandidates = config.getInt("model.max-split-candidates");
    int maxDepth = config.getInt("model.max-depth");
    return new DecisionTree(numFeatures,
                            featuresToTry,
                            minNodeSize,
                            minInfoGainNats,
                            suggestedMaxSplitCandidates,
                            maxDepth,
                            exampleSet);
  }

  public DecisionTree(int numFeatures,
                      int featuresToTry,
                      int minNodeSize,
                      double minInfoGainNats,
                      int suggestedMaxSplitCandidates,
                      int maxDepth,
                      ExampleSet examples) {
    Preconditions.checkArgument(numFeatures >= 1);
    Preconditions.checkArgument(minNodeSize >= 1);
    Preconditions.checkArgument(minInfoGainNats >= 0.0);
    Preconditions.checkArgument(suggestedMaxSplitCandidates >= 1);
    Preconditions.checkArgument(maxDepth >= 1);
    RandomGenerator random = RandomManager.getRandom();
    root = build(examples,
                 1,
                 minNodeSize,
                 minInfoGainNats,
                 featuresToTry,
                 numFeatures,
                 suggestedMaxSplitCandidates,
                 maxDepth,
                 random);
  }

  public DecisionTree(TreeNode root) {
    this.root = root;
  }

  /**
   * @return root node of tree
   */
  public TreeNode getRoot() {
    return root;
  }

  private static TreeNode build(ExampleSet examples,
                                int buildAtDepth,
                                int minNodeSize,
                                double minInfoGainNats,
                                int featuresToTry,
                                int numFeatures,
                                int suggestedMaxSplitCandidates,
                                int maxDepth,
                                RandomGenerator random) {
    if (buildAtDepth >= maxDepth - 1 || examples.getExamples().size() < minNodeSize) {
      return new TerminalNode(Prediction.buildPrediction(examples));
    }

    double bestGain = Double.NEGATIVE_INFINITY;
    Decision bestDecision = null;

    for (int featureNumber : randomFeatures(examples, featuresToTry, numFeatures, random)) {
      Iterable<Decision> decisions =
          Decision.decisionsFromExamples(examples, featureNumber, suggestedMaxSplitCandidates);
      Pair<Decision,Double> decisionAndGain = Information.bestGain(decisions, examples);
      if (decisionAndGain != null) {
        double gain = decisionAndGain.getSecond();
        if (gain > bestGain) {
          bestGain = gain;
          bestDecision = decisionAndGain.getFirst();
        }
      }
    }

    if (Double.isNaN(bestGain) || bestGain < minInfoGainNats) {
      return new TerminalNode(Prediction.buildPrediction(examples));
    }

    bestDecision.setInformationGain(bestGain);

    ExampleSet[] negPosSplit = examples.split(bestDecision);
    examples = null; // For GC?

    TreeNode left = build(negPosSplit[0],
                          buildAtDepth + 1,
                          minNodeSize,
                          minInfoGainNats,
                          featuresToTry,
                          numFeatures,
                          suggestedMaxSplitCandidates,
                          maxDepth,
                          random);
    TreeNode right = build(negPosSplit[1],
                           buildAtDepth + 1,
                           minNodeSize,
                           minInfoGainNats,
                           featuresToTry,
                           numFeatures,
                           suggestedMaxSplitCandidates,
                           maxDepth,
                           random);

    return new DecisionNode(bestDecision, left, right);
  }

  private static Iterable<Integer> randomFeatures(ExampleSet examples,
                                                  int featuresToTry,
                                                  int numFeatures,
                                                  RandomGenerator random) {
    Collection<Integer> features = Sets.newHashSetWithExpectedSize(featuresToTry);
    int max = FastMath.min(numFeatures, featuresToTry);
    int attempts = 0;
    while (features.size() < max && attempts < 2 * featuresToTry) {
      int featureNumber = random.nextInt(numFeatures);
      if (examples.getFeatureType(featureNumber) != FeatureType.IGNORED) {
        features.add(featureNumber);
      }
      attempts++;
    }
    return features;
  }

  @Override
  public Prediction classify(Example test) {
    TerminalNode terminalNode = findTerminal(test);
    return terminalNode.getPrediction();
  }

  private TerminalNode findTerminal(Example example) {
    TreeNode node = root;
    while (!node.isTerminal()) {
      DecisionNode decisionNode = (DecisionNode) node;
      if (decisionNode.getDecision().isPositive(example)) {
        node = decisionNode.getRight();
      } else {
        node = decisionNode.getLeft();
      }
    }
    return (TerminalNode) node;
  }

  void importance(Iterable<Example> testSet, int[] totalCounts, double[] totalGains) {
    for (Example test : testSet) {
      TreeNode node = root;
      while (!node.isTerminal()) {
        DecisionNode decisionNode = (DecisionNode) node;
        Decision decision = decisionNode.getDecision();
        int feature = decision.getFeatureNumber();
        totalCounts[feature]++;
        totalGains[feature] += decision.getInformationGain();
        if (decision.isPositive(test)) {
          node = decisionNode.getRight();
        } else {
          node = decisionNode.getLeft();
        }
      }
    }
  }

  @Override
  public void update(Example train) {
    TerminalNode terminalNode = findTerminal(train);
    terminalNode.update(train);
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (root != null) {
      Deque<Pair<TreeNode,TreePath>> toPrint = new LinkedList<Pair<TreeNode,TreePath>>();
      toPrint.push(new Pair<TreeNode,TreePath>(root, TreePath.EMPTY));
      while (!toPrint.isEmpty()) {
        Pair<TreeNode,TreePath> entry = toPrint.pop();
        TreeNode node = entry.getFirst();
        TreePath path = entry.getSecond();
        int pathLength = path.length();
        for (int i = 0; i < pathLength; i++) {
          if (i == pathLength - 1) {
            result.append(" +-");
          } else {
            result.append(path.isLeftAt(i) ? " | " : "   ");
          }
        }
        result.append(node).append('\n');
        if (node != null && !node.isTerminal()) {
          DecisionNode decisionNode = (DecisionNode) node;
          toPrint.push(new Pair<TreeNode,TreePath>(decisionNode.getRight(), path.extendRight()));
          toPrint.push(new Pair<TreeNode,TreePath>(decisionNode.getLeft(), path.extendLeft()));
        }
      }
    }
    return result.toString();
  }

}
