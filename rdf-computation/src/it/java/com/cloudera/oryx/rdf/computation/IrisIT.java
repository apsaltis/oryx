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

package com.cloudera.oryx.rdf.computation;

import com.google.common.collect.BiMap;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.pmml.DecisionForestPMML;
import com.cloudera.oryx.rdf.common.rule.CategoricalPrediction;
import com.cloudera.oryx.rdf.common.rule.NumericDecision;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.common.tree.DecisionNode;
import com.cloudera.oryx.rdf.common.tree.DecisionTree;
import com.cloudera.oryx.rdf.common.tree.TerminalNode;
import com.cloudera.oryx.rdf.computation.local.RDFLocalGenerationRunner;

/**
 * Tests the random decision forest classifier on the classic
 * <a href="http://archive.ics.uci.edu/ml/datasets/Iris">Iris data set</a>. This contains
 * a few numeric features and a categorical target.
 *
 * @author Sean Owen
 */
public final class IrisIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(IrisIT.class);

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("iris");
  }

  @Test
  public void testPMMLOutput() throws Exception {
    // It's not clear this will actually be deterministic but it will probably be for our purposes
    new RDFLocalGenerationRunner().call();
    File pmmlFile = new File(TEST_TEMP_BASE_DIR, "00000/model.pmml.gz");
    Pair<DecisionForest,Map<Integer,BiMap<String,Integer>>> forestAndMapping = DecisionForestPMML.read(pmmlFile);
    DecisionForest forest = forestAndMapping.getFirst();
    Map<Integer,BiMap<String,Integer>> categoryValueMapping = forestAndMapping.getSecond();
    Map<String,Integer> targetCategoryValueMapping = categoryValueMapping.get(4);

    log.info("{}", forest);

    // Simple tests of the structure:

    DecisionTree[] trees = forest.getTrees();
    assertEquals(2, trees.length);

    DecisionTree tree0;
    DecisionTree tree1;

    double[] weights = forest.getWeights();
    if (weights[0] == 0.8904109589041096) {
      assertEquals(0.922077922077922, weights[1]);
      tree0 = trees[0];
      tree1 = trees[1];
    } else if (weights[1] == 0.8904109589041096) {
      assertEquals(0.922077922077922, weights[0]);
      tree0 = trees[1];
      tree1 = trees[0];
    } else {
      fail(Arrays.toString(weights));
      return;
    }

    DecisionNode root0 = (DecisionNode) tree0.getRoot();
    NumericDecision decision0 = (NumericDecision) root0.getDecision();
    assertEquals(4.8500004f, decision0.getThreshold());
    assertEquals(2, decision0.getFeatureNumber());

    DecisionNode root1 = (DecisionNode) tree1.getRoot();
    NumericDecision decision1 = (NumericDecision) root1.getDecision();
    assertEquals(2.75f, decision1.getThreshold());
    assertEquals(2, decision1.getFeatureNumber());

    TerminalNode root1Neg = (TerminalNode) root1.getLeft();
    assertEquals(34, root1Neg.getCount());
    CategoricalPrediction root1NegPrediction = (CategoricalPrediction) root1Neg.getPrediction();
    assertArrayEquals(new int[] {0,34,0}, root1NegPrediction.getCategoryCounts());
    assertArrayEquals(new float[] {0.0f, 1.0f, 0.0f}, root1NegPrediction.getCategoryProbabilities());

    Feature[] features = {
        //5.9,3.0,5.1,1.8,Iris-virginica
        NumericFeature.forValue(5.9f),
        NumericFeature.forValue(3.0f),
        NumericFeature.forValue(5.1f),
        NumericFeature.forValue(1.8f),
    };
    Example example = new Example(null, features);
    CategoricalPrediction prediction = (CategoricalPrediction) forest.classify(example);
    int expectedCategory = targetCategoryValueMapping.get("Iris-virginica");
    assertEquals(expectedCategory, prediction.getMostProbableCategoryID());
    float[] expectedProbabilities = new float[3];
    expectedProbabilities[expectedCategory] = 1.0f;
     assertArrayEquals(expectedProbabilities, prediction.getCategoryProbabilities());
  }

}
