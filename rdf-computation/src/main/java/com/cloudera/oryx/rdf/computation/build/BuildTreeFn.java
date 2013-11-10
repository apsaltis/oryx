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

package com.cloudera.oryx.rdf.computation.build;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.typesafe.config.Config;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.FastMath;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import com.cloudera.oryx.rdf.common.eval.Evaluation;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.IgnoredFeature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.pmml.DecisionForestPMML;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.common.tree.DecisionTree;

/**
 * @author Sean Owen
 */
public final class BuildTreeFn extends OryxReduceDoFn<Integer, Iterable<String>, String> {

  private static final Logger log = LoggerFactory.getLogger(BuildTreeFn.class);

  private int numTreesToBuild;
  private int totalFoldsPerTree;
  private final RandomGenerator random = RandomManager.getRandom();

  @Override
  public void initialize() {
    super.initialize();
    int numReducers = getContext().getNumReduceTasks();
    log.info("{} reducers", numReducers);

    Config config = ConfigUtils.getDefaultConfig();
    int numTrees = config.getInt("model.num-trees");
    // Bump this up to as least 2x reducers
    numTrees = FastMath.max(numTrees, 2 * numReducers);
    // Make it a multiple of # reducers
    while ((numTrees % numReducers) != 0) {
      numTrees++;
    }
    log.info("{} total trees", numTrees);

    // Base assumption is 1 fold per tree
    // Going to build this many trees per reducer -- therefore will send this many folds to each reducer.
    // This is the minimum amount of data each tree gets to use:
    int foldsPerReducer = numTrees / numReducers;
    numTreesToBuild = foldsPerReducer;
    log.info("Building {} trees locally", numTreesToBuild);

    // Sending each datum to multiple reducers means trees see more data. Keep increasing it until the desired
    // sample rate is met or exceeded.
    double sampleRate = config.getDouble("model.sample-rate");
    int reducersPerDatum = 1;
    // -1 here because one fold is used for validation
    while ((double) (reducersPerDatum * foldsPerReducer - 1) / numTrees < sampleRate) {
      reducersPerDatum++;
    }

    // Handle case of 100% sampling, where this would run over. Can't send more than all folds to all reducers:
    totalFoldsPerTree = FastMath.min(numTrees, reducersPerDatum * foldsPerReducer);
    log.info("{} folds per tree", totalFoldsPerTree);
  }

  @Override
  public void process(Pair<Integer,Iterable<String>> input, Emitter<String> emitter) {

    InboundSettings inboundSettings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    int numFeaturesAndTarget = inboundSettings.getColumnNames().size();
    Integer targetColumn = inboundSettings.getTargetColumn();
    Preconditions.checkNotNull(targetColumn, "No target-column specified");

    List<Example> training = Lists.newArrayList();
    List<Example> cvSet = Lists.newArrayList();
    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping = Maps.newHashMap();

    log.info("Reading input");
    for (String line : input.second()) {
      String[] tokens = DelimitedDataUtils.decode(line);
      Feature target = null;
      Feature[] features = new Feature[numFeaturesAndTarget]; // Too big by 1 but makes math easier
      for (int col = 0; col < numFeaturesAndTarget; col++) {
        if (col == targetColumn) {
          target = buildFeature(col, tokens[col], inboundSettings, columnToCategoryNameToIDMapping);
          features[col] = IgnoredFeature.INSTANCE;
        } else {
          features[col] = buildFeature(col, tokens[col], inboundSettings, columnToCategoryNameToIDMapping);
        }
      }
      Preconditions.checkNotNull(target);
      Example example = new Example(target, features);
      if (random.nextInt(totalFoldsPerTree) == 0) {
        cvSet.add(example);
      } else {
        training.add(example);
      }
    }

    if (cvSet.isEmpty() && training.isEmpty()) {
      return;
    }

    ExampleSet cvExampleSet = new ExampleSet(cvSet);
    DecisionTree[] trees = new DecisionTree[numTreesToBuild];
    double[] weights = new double[trees.length];
    Arrays.fill(weights, 1.0);

    for (int i = 0; i < numTreesToBuild; i++) {
      trees[i] = DecisionTree.fromExamplesWithDefault(training);
      progress(); // Helps prevent timeouts
      log.info("Built tree {}", i);
      double[] weightEval = Evaluation.evaluateToWeight(trees[i], cvExampleSet);
      weights[i] = weightEval[0];
      progress(); // Helps prevent timeouts
      log.info("Evaluated tree {}", i);
    }

    DecisionForest forest = new DecisionForest(trees, weights);

    log.info("Writing forest to file");
    String pmmlFileContents;
    try  {
      File tempFile = File.createTempFile("model-", ".pmml.gz");
      tempFile.deleteOnExit();
      DecisionForestPMML.write(tempFile, forest, columnToCategoryNameToIDMapping);
      Reader in = IOUtils.openReaderMaybeDecompressing(tempFile);
      try {
        pmmlFileContents = CharStreams.toString(in);
      } finally {
        in.close();
      }
      IOUtils.delete(tempFile);
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }

    log.info("Emitting forest");
    emitter.emit(pmmlFileContents);
  }

  private static Feature buildFeature(int columnNumber,
                                      String token,
                                      InboundSettings inboundSettings,
                                      Map<Integer, BiMap<String, Integer>> columnToCategoryNameToIDMapping) {
    if (inboundSettings.isNumeric(columnNumber)) {
      return NumericFeature.forValue(Float.parseFloat(token));
    }
    if (inboundSettings.isCategorical(columnNumber)) {
      return CategoricalFeature.forValue(categoricalFromString(columnNumber, token, columnToCategoryNameToIDMapping));
    }
    return IgnoredFeature.INSTANCE;
  }

  private static int categoricalFromString(int columnNumber,
                                           String value,
                                           Map<Integer, BiMap<String, Integer>> columnToCategoryNameToIDMapping) {
    BiMap<String,Integer> categoryNameToID = columnToCategoryNameToIDMapping.get(columnNumber);
    if (categoryNameToID == null) {
      categoryNameToID = HashBiMap.create();
      columnToCategoryNameToIDMapping.put(columnNumber, categoryNameToID);
    }
    Integer mapped = categoryNameToID.get(value);
    if (mapped != null) {
      return mapped;
    }
    int newCategory = categoryNameToID.size();
    categoryNameToID.put(value, newCategory);
    return newCategory;
  }

}
