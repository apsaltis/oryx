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

package com.cloudera.oryx.rdf.serving.web;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.io.CharStreams;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.IgnoredFeature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.tree.TreeBasedClassifier;
import com.cloudera.oryx.rdf.serving.generation.Generation;
import com.cloudera.oryx.rdf.serving.generation.RDFGenerationManager;

/**
 * <p>Responsds to POST request to {@code /train}. The input is one or more data points
 * to train, one for each line of the request body. Each data point is a delimited line of input like
 * "1,foo,3.0". The classifier updates to learn in some way from the new data. The response is empty.</p>
 *
 * @author Sean Owen
 */
public final class TrainServlet extends AbstractRDFServlet {

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

    RDFGenerationManager generationManager = getGenerationManager();
    Generation generation = generationManager.getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return;
    }

    InboundSettings inboundSettings = getInboundSettings();

    TreeBasedClassifier forest = generation.getForest();
    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping =
        generation.getColumnToCategoryNameToIDMapping();

    int totalColumns = getTotalColumns();

    for (CharSequence line : CharStreams.readLines(request.getReader())) {

      generationManager.append(line);

      String[] tokens = DelimitedDataUtils.decode(line);
      if (tokens.length != totalColumns) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
        return;
      }

      Feature target = null;
      Feature[] features = new Feature[totalColumns]; // Too big by 1 but makes math easier
      try {
        for (int col = 0; col < features.length; col++) {
          if (col == inboundSettings.getTargetColumn()) {
            target = buildFeature(col, tokens[col], columnToCategoryNameToIDMapping);
            features[col] = IgnoredFeature.INSTANCE;
          } else {
            features[col] = buildFeature(col, tokens[col], columnToCategoryNameToIDMapping);
          }
        }
      } catch (IllegalArgumentException iae) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad input line");
        return;
      }

      Preconditions.checkNotNull(target);
      Example example = new Example(target, features);

      forest.update(example);

    }
  }

  private Feature buildFeature(int columnNumber,
                               String token,
                               Map<Integer, BiMap<String, Integer>> columnToCategoryNameToIDMapping) {
    InboundSettings inboundSettings = getInboundSettings();
    if (inboundSettings.isNumeric(columnNumber)) {
      return NumericFeature.forValue(Float.parseFloat(token));
    }
    if (inboundSettings.isCategorical(columnNumber)) {
      return CategoricalFeature.forValue(
          ClassifyServlet.categoricalFromString(columnNumber, token, columnToCategoryNameToIDMapping));
    }
    return IgnoredFeature.INSTANCE;
  }

}
