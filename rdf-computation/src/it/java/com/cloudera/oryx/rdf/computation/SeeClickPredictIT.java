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
import java.util.Map;

import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.IgnoredFeature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.pmml.DecisionForestPMML;
import com.cloudera.oryx.rdf.common.rule.NumericPrediction;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.computation.local.RDFLocalGenerationRunner;

/**
 * Test based on data from the
 * <a href="http://www.kaggle.com/c/see-click-predict-fix">Kaggle See-Click-Predict</a> competition.
 * Not for redistribution.
 *
 * @author Sean Owen
 */
public final class SeeClickPredictIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(SeeClickPredictIT.class);

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("seeclickpredict");
  }

  @Test
  public void testOutput() throws Exception {
    new RDFLocalGenerationRunner().call();
    File pmmlFile = new File(TEST_TEMP_BASE_DIR, "00000/model.pmml.gz");
    Pair<DecisionForest,Map<Integer,BiMap<String,Integer>>> forestAndMapping = DecisionForestPMML.read(pmmlFile);
    DecisionForest forest = forestAndMapping.getFirst();
    Feature[] features = {
        //"343122","37.538814","-77.437136","Pothole in Crosswalk","...","2","0","18","New Map Widget","2012-01-01 14:05:30","pothole"
        NumericFeature.forValue(123456),
        NumericFeature.forValue(37.5f),
        NumericFeature.forValue(-77.4f),
        IgnoredFeature.INSTANCE,
        IgnoredFeature.INSTANCE,
        IgnoredFeature.INSTANCE,
        IgnoredFeature.INSTANCE,
        IgnoredFeature.INSTANCE,
        CategoricalFeature.forValue(0),
        IgnoredFeature.INSTANCE,
        CategoricalFeature.forValue(0),
    };
    Example example = new Example(null, features);
    NumericPrediction prediction = (NumericPrediction) forest.classify(example);
    float predicted = prediction.getPrediction();
    log.info("Prediction: {}", predicted);
    // Really just checking for errors -- allow a wide, wide margin of error
    assertEquals(20, predicted, 15);
  }

}
