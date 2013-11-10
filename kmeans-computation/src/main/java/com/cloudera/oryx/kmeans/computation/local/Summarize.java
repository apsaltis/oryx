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

package com.cloudera.oryx.kmeans.computation.local;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.computation.common.summary.InternalStats;
import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.computation.common.summary.SummaryStats;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

public final class Summarize implements Callable<Summary> {

  private static final Logger log = LoggerFactory.getLogger(Summarize.class);

  private final File inputDir;

  public Summarize(File inputDir) {
    this.inputDir = inputDir;
  }

  @Override
  public Summary call() throws IOException {
    File[] inputFiles = inputDir.listFiles(IOUtils.CSV_COMPRESSED_FILTER);
    if (inputFiles == null || inputFiles.length == 0) {
      log.info("No .csv input files in {}", inputDir);
      return null;
    }


    InboundSettings inboundSettings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    int numFeatures = inboundSettings.getColumnNames().size();
    List<InternalStats> internalStats = Lists.newArrayListWithExpectedSize(numFeatures);
    for (int col = 0; col < numFeatures; col++) {
      if (inboundSettings.isCategorical(col) || inboundSettings.isNumeric(col)) {
        internalStats.add(new InternalStats());
      } else {
        internalStats.add(null);
      }
    }
    int totalRecords = 0;

    for (File inputFile : inputFiles) {
      log.info("Summarizing input from {}", inputFile.getName());
      for (String line : new FileLineIterable(inputFile)) {
        if (line.isEmpty()) {
          continue;
        }
        totalRecords++;
        String[] tokens = DelimitedDataUtils.decode(line);
        for (int col = 0; col < numFeatures; col++) {
          if (!inboundSettings.isIgnored(col)) {
            if (inboundSettings.isCategorical(col)) {
              internalStats.get(col).addCategorical(tokens[col]);
            } else if (inboundSettings.isNumeric(col)) {
              internalStats.get(col).addNumeric(Double.valueOf(tokens[col]));
            }
          }
        }
      }
    }

    List<SummaryStats> stats = Lists.newArrayListWithExpectedSize(numFeatures);
    for (int col = 0; col < numFeatures; col++) {
      InternalStats internal = internalStats.get(col);
      if (internal != null) {
        stats.add(internal.toSummaryStats(inboundSettings.getColumnNames().get(col), totalRecords));
      } else {
        stats.add(null);
      }
    }

    return new Summary(totalRecords, numFeatures, stats);
  }
}
