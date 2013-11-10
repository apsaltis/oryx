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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.example.IgnoredFeature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.rule.CategoricalPrediction;
import com.cloudera.oryx.rdf.common.rule.NumericPrediction;
import com.cloudera.oryx.rdf.common.rule.Prediction;
import com.cloudera.oryx.rdf.common.tree.TreeBasedClassifier;
import com.cloudera.oryx.rdf.serving.generation.Generation;

/**
 * <p>Responsds to a GET request to {@code /classify/[datum]}. The input is one data point to classify,
 * delimited, like "1,foo,3.0". The response body contains the result of classification on one line.
 * The result depends on the classifier --  could be a number or a category name.</p>
 *
 * @author Sean Owen
 */
public final class ClassifyServlet extends AbstractRDFServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    String line = pathInfo.subSequence(1, pathInfo.length()).toString();

    Generation generation = getGenerationManager().getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return;
    }

    InboundSettings inboundSettings = getInboundSettings();

    TreeBasedClassifier forest = generation.getForest();
    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping =
        generation.getColumnToCategoryNameToIDMapping();

    Map<Integer,String> targetIDToCategory =
        columnToCategoryNameToIDMapping.get(inboundSettings.getTargetColumn()).inverse();

    Writer out = response.getWriter();

    int totalColumns = getTotalColumns();

    String[] tokens = DelimitedDataUtils.decode(line);
    if (tokens.length != totalColumns) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
      return;
    }

    Feature[] features = new Feature[totalColumns]; // Too big by 1 but makes math easier

    try {
      for (int col = 0; col < features.length; col++) {
        if (col == inboundSettings.getTargetColumn()) {
          features[col] = IgnoredFeature.INSTANCE;
        } else {
          features[col] = buildFeature(col, tokens[col], columnToCategoryNameToIDMapping);
        }
      }
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad input line");
      return;
    }

    Example example = new Example(null, features);
    Prediction prediction = forest.classify(example);
    if (prediction.getFeatureType() == FeatureType.CATEGORICAL) {
      int categoryID = ((CategoricalPrediction) prediction).getMostProbableCategoryID();
      out.write(targetIDToCategory.get(categoryID));
    } else {
      out.write(Float.toString(((NumericPrediction) prediction).getPrediction()));
    }
    out.write("\n");
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
          categoricalFromString(columnNumber, token, columnToCategoryNameToIDMapping));
    }
    return IgnoredFeature.INSTANCE;
  }

  static int categoricalFromString(int columnNumber,
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
