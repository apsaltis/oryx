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

package com.cloudera.oryx.rdf.common.pmml;

import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.math3.util.Pair;
import org.dmg.pmml.Array;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.MissingValueStrategyType;
import org.dmg.pmml.Model;
import org.dmg.pmml.MultipleModelMethodType;
import org.dmg.pmml.Node;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.Segment;
import org.dmg.pmml.Segmentation;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;
import org.dmg.pmml.TypeDefinitionField;
import org.dmg.pmml.Value;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.cloudera.oryx.common.collection.BitSet;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.rule.CategoricalDecision;
import com.cloudera.oryx.rdf.common.rule.CategoricalPrediction;
import com.cloudera.oryx.rdf.common.rule.Decision;
import com.cloudera.oryx.rdf.common.rule.NumericDecision;
import com.cloudera.oryx.rdf.common.rule.NumericPrediction;
import com.cloudera.oryx.rdf.common.rule.Prediction;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.common.tree.DecisionNode;
import com.cloudera.oryx.rdf.common.tree.DecisionTree;
import com.cloudera.oryx.rdf.common.tree.TerminalNode;
import com.cloudera.oryx.rdf.common.tree.TreeNode;

/**
 * Contains utility methods for writing a {@link DecisionTree} as a PMML file, and reading it back.
 *
 * @author Sean Owen
 */
public final class DecisionForestPMML {

  private DecisionForestPMML() {
  }


  // Write PMML

  /**
   * @param pmmlFile file to write PMML representation to
   * @param forest {@link DecisionForest} to encode as PMML
   * @param columnToCategoryNameToIDMapping {@link Map} from column number in the input, to a {@link BiMap}
   *  mapping between category value names and category value IDs (for categorical feature columns only). This
   *  is necessary because the {@link DecisionForest} operates in terms of value IDs, but the PMML encoding
   *  should encode the names of these category values -- {@code female}, not {@code 2}
   */
  public static void write(File pmmlFile,
                           DecisionForest forest,
                           Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping) throws IOException {

    InboundSettings inboundSettings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    List<String> columnNames = inboundSettings.getColumnNames();
    int targetColumn = inboundSettings.getTargetColumn();
    boolean classificationTask = inboundSettings.isCategorical(targetColumn);

    MiningFunctionType miningFunctionType =
        classificationTask ? MiningFunctionType.CLASSIFICATION : MiningFunctionType.REGRESSION;
    MiningSchema miningSchema = PMMLUtils.buildMiningSchema(inboundSettings, columnNames, targetColumn);
    MiningModel miningModel = new MiningModel(miningSchema, miningFunctionType);
    MultipleModelMethodType multipleModelMethodType = classificationTask ?
        MultipleModelMethodType.WEIGHTED_MAJORITY_VOTE :
        MultipleModelMethodType.WEIGHTED_AVERAGE;
    Segmentation segmentation = new Segmentation(multipleModelMethodType);
    miningModel.setSegmentation(segmentation);

    int treeID = 0;
    for (DecisionTree tree : forest) {
      Segment segment = buildTreeModel(forest,
                                       columnToCategoryNameToIDMapping,
                                       miningFunctionType,
                                       miningSchema,
                                       treeID,
                                       tree,
                                       inboundSettings);
      segmentation.getSegments().add(segment);
      treeID++;
    }

    DataDictionary dictionary = PMMLUtils.buildDataDictionary(inboundSettings, columnToCategoryNameToIDMapping);
    PMML pmml = new PMML(null, dictionary, "4.1");
    pmml.getModels().add(miningModel);

    OutputStream out = IOUtils.buildGZIPOutputStream(new FileOutputStream(pmmlFile));
    try {
      IOUtil.marshal(pmml, out);
    } catch (JAXBException jaxbe) {
      throw new IOException(jaxbe);
    } finally {
      out.close();
    }
  }

  private static Segment buildTreeModel(DecisionForest forest,
                                        Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping,
                                        MiningFunctionType miningFunctionType,
                                        MiningSchema miningSchema,
                                        int treeID,
                                        DecisionTree tree,
                                        InboundSettings settings) {

    List<String> columnNames = settings.getColumnNames();
    int targetColumn = settings.getTargetColumn();

    Node root = new Node();
    root.setId("r");

    // Queue<Node> modelNodes = Queues.newArrayDeque();
    Queue<Node> modelNodes = new ArrayDeque<Node>();
    modelNodes.add(root);

    Queue<Pair<TreeNode, Decision>> treeNodes = new ArrayDeque<Pair<TreeNode, Decision>>();
    treeNodes.add(new Pair<TreeNode,Decision>(tree.getRoot(), null));

    while (!treeNodes.isEmpty()) {

      Pair<TreeNode,Decision> treeNodePredicate = treeNodes.remove();
      Node modelNode = modelNodes.remove();

      // This is the decision that got us here from the parent, if any; not the predicate at this node
      Predicate predicate = buildPredicate(treeNodePredicate.getSecond(),
                                           columnNames,
                                           columnToCategoryNameToIDMapping);
      modelNode.setPredicate(predicate);

      TreeNode treeNode = treeNodePredicate.getFirst();
      if (treeNode.isTerminal()) {

        TerminalNode terminalNode = (TerminalNode) treeNode;
        modelNode.setRecordCount((double) terminalNode.getCount());

        Prediction prediction = terminalNode.getPrediction();

        if (prediction.getFeatureType() == FeatureType.CATEGORICAL) {

          Map<Integer,String> categoryIDToName =
              columnToCategoryNameToIDMapping.get(targetColumn).inverse();
          CategoricalPrediction categoricalPrediction = (CategoricalPrediction) prediction;
          int[] categoryCounts = categoricalPrediction.getCategoryCounts();
          float[] categoryProbabilities = categoricalPrediction.getCategoryProbabilities();
          for (int categoryID = 0; categoryID < categoryProbabilities.length; categoryID++) {
            int categoryCount = categoryCounts[categoryID];
            float probability = categoryProbabilities[categoryID];
            if (categoryCount > 0 && probability > 0.0f) {
              String categoryName = categoryIDToName.get(categoryID);
              ScoreDistribution distribution = new ScoreDistribution(categoryName, categoryCount);
              distribution.setProbability((double) probability);
              modelNode.getScoreDistributions().add(distribution);
            }
          }

        } else {

          NumericPrediction numericPrediction = (NumericPrediction) prediction;
          modelNode.setScore(Float.toString(numericPrediction.getPrediction()));
        }

      } else {

        DecisionNode decisionNode = (DecisionNode) treeNode;
        Decision decision = decisionNode.getDecision();

        Node positiveModelNode = new Node();
        positiveModelNode.setId(modelNode.getId() + '+');
        modelNode.getNodes().add(positiveModelNode);
        Node negativeModelNode = new Node();
        negativeModelNode.setId(modelNode.getId() + '-');
        modelNode.getNodes().add(negativeModelNode);
        modelNode.setDefaultChild(
            decision.getDefaultDecision() ? positiveModelNode.getId() : negativeModelNode.getId());
        modelNodes.add(positiveModelNode);
        modelNodes.add(negativeModelNode);
        treeNodes.add(new Pair<TreeNode,Decision>(decisionNode.getRight(), decision));
        treeNodes.add(new Pair<TreeNode,Decision>(decisionNode.getLeft(), null));

      }

    }

    TreeModel treeModel = new TreeModel(miningSchema, root, miningFunctionType);
    treeModel.setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT);
    treeModel.setMissingValueStrategy(MissingValueStrategyType.DEFAULT_CHILD);

    Segment segment = new Segment();
    segment.setId(Integer.toString(treeID));
    segment.setPredicate(new True());
    segment.setModel(treeModel);
    segment.setWeight(forest.getWeights()[treeID]);

    return segment;
  }

  private static Predicate buildPredicate(Decision decision,
                                          List<String> columnNames,
                                          Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping) {
    Predicate predicate;
    if (decision == null) {
      predicate = new True();

    } else {
      int columnNumber = decision.getFeatureNumber();
      FieldName fieldName = new FieldName(columnNames.get(columnNumber));

      if (decision.getType() == FeatureType.CATEGORICAL) {
        CategoricalDecision categoricalDecision = (CategoricalDecision) decision;
        Map<Integer,String> categoryIDToName = columnToCategoryNameToIDMapping.get(columnNumber).inverse();
        BitSet includedCategoryIDs = categoricalDecision.getCategoryIDs();
        List<String> categoryNames = Lists.newArrayList();
        int categoryID = -1;
        while ((categoryID = includedCategoryIDs.nextSetBit(categoryID + 1)) >= 0) {
          categoryNames.add(categoryIDToName.get(categoryID));
        }
        Array categories = new Array(DelimitedDataUtils.encode(categoryNames, ' '), Array.Type.STRING);
        predicate = new SimpleSetPredicate(categories, fieldName, SimpleSetPredicate.BooleanOperator.IS_IN);

      } else {
        NumericDecision numericDecision = (NumericDecision) decision;
        SimplePredicate numericPredicate = new SimplePredicate(fieldName, SimplePredicate.Operator.GREATER_OR_EQUAL);
        numericPredicate.setValue(Float.toString(numericDecision.getThreshold()));
        predicate = numericPredicate;
      }
    }
    return predicate;
  }


  // Read PMML

  /**
   * @param pmmlFile file to read PMML encoding from
   * @return a {@link DecisionForest} representation of the PMML encoded model
   */
  public static Pair<DecisionForest, Map<Integer,BiMap<String,Integer>>> read(File pmmlFile) throws IOException {

    PMML pmml;
    InputStream in = IOUtils.openMaybeDecompressing(pmmlFile);
    try {
      pmml = IOUtil.unmarshal(in);
    } catch (SAXException e) {
      throw new IOException(e);
    } catch (JAXBException e) {
      throw new IOException(e);
    } finally {
      in.close();
    }

    List<Model> models = pmml.getModels();
    Preconditions.checkNotNull(models);
    Preconditions.checkArgument(!models.isEmpty());
    Preconditions.checkArgument(models.get(0) instanceof MiningModel);
    MiningModel miningModel = (MiningModel) models.get(0);

    Segmentation segmentation = miningModel.getSegmentation();
    Preconditions.checkNotNull(segmentation);

    List<Segment> segments = segmentation.getSegments();
    Preconditions.checkNotNull(segments);
    Preconditions.checkArgument(!segments.isEmpty());

    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping = PMMLUtils.buildColumnCategoryMapping(
        pmml.getDataDictionary());
    InboundSettings settings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    DecisionTree[] trees = new DecisionTree[segments.size()];
    double[] weights = new double[trees.length];
    for (int i = 0; i < trees.length; i++) {
      Segment segment = segments.get(i);
      weights[i] = segment.getWeight();
      TreeModel treeModel = (TreeModel) segment.getModel();
      TreeNode root = translateFromPMML(treeModel.getNode(), columnToCategoryNameToIDMapping, settings);
      trees[i] = new DecisionTree(root);
    }

    return new Pair<DecisionForest, Map<Integer, BiMap<String, Integer>>>(
        new DecisionForest(trees, weights),
        columnToCategoryNameToIDMapping);
  }

  private static TreeNode translateFromPMML(Node root,
                                            Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping,
                                            InboundSettings settings) {

    List<String> columnNames = settings.getColumnNames();
    int targetColumn = settings.getTargetColumn();

    List<Node> children = root.getNodes();
    if (children.isEmpty()) {
      // Terminal
      Collection<ScoreDistribution> scoreDistributions = root.getScoreDistributions();
      Prediction prediction;
      if (scoreDistributions != null && !scoreDistributions.isEmpty()) {
        // Categorical target
        Map<String,Integer> valueToID = columnToCategoryNameToIDMapping.get(targetColumn);
        int[] categoryCounts = new int[valueToID.size()];
        for (ScoreDistribution dist : scoreDistributions) {
          int valueID = valueToID.get(dist.getValue());
          categoryCounts[valueID] = (int) Math.round(dist.getRecordCount());
        }
        prediction = new CategoricalPrediction(categoryCounts);
      } else {
        prediction = new NumericPrediction(Float.parseFloat(root.getScore()), (int) Math.round(root.getRecordCount()));
      }
      return new TerminalNode(prediction);
    }

    Preconditions.checkArgument(children.size() == 2);
    // Decision
    Node child1 = children.get(0);
    Node child2 = children.get(1);
    Node negativeLeftChild;
    Node positiveRightChild;
    if (child1.getPredicate().getClass().equals(True.class)) {
      negativeLeftChild = child1;
      positiveRightChild = child2;
    } else {
      Preconditions.checkArgument(child2.getPredicate().getClass().equals(True.class));
      negativeLeftChild = child2;
      positiveRightChild = child1;
    }

    Decision decision;
    Predicate predicate = positiveRightChild.getPredicate();
    boolean defaultDecision = positiveRightChild.getId().equals(root.getDefaultChild());

    if (predicate instanceof SimplePredicate) {
      // Numeric decision
      SimplePredicate simplePredicate = (SimplePredicate) predicate;
      Preconditions.checkArgument(simplePredicate.getOperator() == SimplePredicate.Operator.GREATER_OR_EQUAL);
      float threshold = Float.parseFloat(simplePredicate.getValue());
      int featureNumber = columnNames.indexOf(simplePredicate.getField().getValue());
      decision = new NumericDecision(featureNumber, threshold, defaultDecision);

    } else {
      // Cateogrical decision
      Preconditions.checkArgument(predicate instanceof SimpleSetPredicate);
      SimpleSetPredicate simpleSetPredicate = (SimpleSetPredicate) predicate;
      Preconditions.checkArgument(simpleSetPredicate.getBooleanOperator() == SimpleSetPredicate.BooleanOperator.IS_IN);
      int featureNumber = columnNames.indexOf(simpleSetPredicate.getField().getValue());
      Map<String,Integer> categoryNameToID = columnToCategoryNameToIDMapping.get(featureNumber);
      String[] categories = DelimitedDataUtils.decode(simpleSetPredicate.getArray().getValue(), ' ');
      BitSet activeCategories = new BitSet(categoryNameToID.size());
      for (String category : categories) {
        int categoryID = categoryNameToID.get(category);
        activeCategories.set(categoryID);
      }
      decision = new CategoricalDecision(featureNumber, activeCategories, defaultDecision);
    }

    return new DecisionNode(decision,
                            translateFromPMML(negativeLeftChild, columnToCategoryNameToIDMapping, settings),
                            translateFromPMML(positiveRightChild, columnToCategoryNameToIDMapping, settings));
  }

}
