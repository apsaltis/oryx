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

package com.cloudera.oryx.rdf.computation.local;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.IgnoredFeature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;

final class ReadInputs implements Callable<Object> {

  private static final Logger log = LoggerFactory.getLogger(ReadInputs.class);

  private final File inputDir;
  private final Collection<Example> examples;
  private final Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping;

  ReadInputs(File inputDir,
             Collection<Example> examples,
             Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping) {
    this.inputDir = inputDir;
    this.examples = examples;
    this.columnToCategoryNameToIDMapping = columnToCategoryNameToIDMapping;
  }

  @Override
  public Void call() throws IOException {
    File[] inputFiles = inputDir.listFiles(IOUtils.CSV_COMPRESSED_FILTER);
    if (inputFiles == null) {
      log.info("No input files in {}", inputDir);
      return null;
    }

    InboundSettings inboundSettings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    int numFeaturesAndTarget = inboundSettings.getColumnNames().size();
    Integer targetColumn = inboundSettings.getTargetColumn();
    Preconditions.checkNotNull(targetColumn, "No target-column specified");

    for (File inputFile : inputFiles) {
      log.info("Reading input from {}", inputFile);
      for (String line : new FileLineIterable(inputFile)) {
        if (line.isEmpty()) {
          continue;
        }
        String[] tokens = DelimitedDataUtils.decode(line);
        Feature target = null;
        Feature[] features = new Feature[numFeaturesAndTarget]; // Too big by 1 but makes math easier
        for (int col = 0; col < numFeaturesAndTarget; col++) {
          if (col == targetColumn) {
            target = buildFeature(col, tokens[col], inboundSettings);
            features[col] = IgnoredFeature.INSTANCE;
          } else {
            features[col] = buildFeature(col, tokens[col], inboundSettings);
          }
        }
        Preconditions.checkNotNull(target);
        examples.add(new Example(target, features));
      }
    }

    return null;
  }

  private Feature buildFeature(int columnNumber, String token, InboundSettings inboundSettings) {
    if (inboundSettings.isNumeric(columnNumber)) {
      return NumericFeature.forValue(Float.parseFloat(token));
    }
    if (inboundSettings.isCategorical(columnNumber)) {
      return CategoricalFeature.forValue(categoricalFromString(columnNumber, token));
    }
    return IgnoredFeature.INSTANCE;
  }

  private int categoricalFromString(int columnNumber, String value) {
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
