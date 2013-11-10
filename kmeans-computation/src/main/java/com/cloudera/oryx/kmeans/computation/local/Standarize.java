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
import com.cloudera.oryx.common.math.NamedRealVector;
import com.cloudera.oryx.common.math.Vectors;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.computation.common.crossfold.Crossfold;
import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.computation.common.summary.SummaryStats;
import com.cloudera.oryx.kmeans.computation.normalize.NormalizeSettings;
import com.cloudera.oryx.kmeans.computation.normalize.Transform;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

public final class Standarize implements Callable<List<List<RealVector>>> {

  private static final Logger log = LoggerFactory.getLogger(Standarize.class);

  private final File inputDir;
  private final Summary summary;

  public Standarize(File inputDir, Summary summary) {
    this.inputDir = inputDir;
    this.summary = summary;
  }

  @Override
  public List<List<RealVector>> call() throws IOException {
    File[] inputFiles = inputDir.listFiles(IOUtils.CSV_COMPRESSED_FILTER);
    if (inputFiles == null || inputFiles.length == 0) {
      log.info("No .csv input files in {}", inputDir);
      return null;
    }

    Config config = ConfigUtils.getDefaultConfig();
    InboundSettings inboundSettings = InboundSettings.create(config);
    int numFeatures = inboundSettings.getColumnNames().size();
    NormalizeSettings settings = NormalizeSettings.create(config);
    Crossfold crossfold = new Crossfold(config.getInt("model.cross-folds"));
    RandomGenerator rand = crossfold.getRandomGenerator();

    Collection<Integer> ignoredColumns = inboundSettings.getIgnoredColumns();
    Collection<Integer> idColumns = inboundSettings.getIdColumns();
    int expansion = -ignoredColumns.size() + summary.getNetLevels() -
        (!idColumns.isEmpty() && !ignoredColumns.contains(idColumns.iterator().next()) ? 1 : 0);
    boolean sparse = settings.getSparse() != null ? settings.getSparse() :
        expansion > 2 * (summary.getFieldCount() - ignoredColumns.size());

    List<List<RealVector>> ret = Lists.newArrayList();
    for (int i = 0; i < crossfold.getNumFolds(); i++) {
      ret.add(Lists.<RealVector>newLinkedList());
    }
    for (File inputFile : inputFiles) {
      log.info("Standardizing input from {}", inputFile.getName());
      for (String line : new FileLineIterable(inputFile)) {
        if (line.isEmpty()) {
          continue;
        }
        String[] tokens = DelimitedDataUtils.decode(line);
        RealVector v = sparse ? Vectors.sparse(tokens.length + expansion) : Vectors.dense(tokens.length + expansion);
        int offset = 0;
        for (int i = 0; i < numFeatures; i++) {
          if (inboundSettings.isIgnored(i)) {
            // Do nothing
          } else if (inboundSettings.isNumeric(i)) {
            SummaryStats ss = summary.getStats(i);
            Transform t = settings.getTransform(i);
            double raw = asDouble(tokens[i]);
            if (!Double.isNaN(raw)) {
              double n = t.apply(raw, ss) * settings.getScale(i);
              v.setEntry(offset, n);
            }
            offset++;
          } else if (inboundSettings.isCategorical(i)) {
            SummaryStats ss = summary.getStats(i);
            int index = ss.index(tokens[i]);
            if (index >= 0) {
              v.setEntry(offset + index, settings.getScale(i));
              offset += ss.numLevels();
            } else {
              log.warn("Unrecognized value for category {}: {}", i, tokens[i]);
            }
          }
        }
        if (!inboundSettings.getIdColumns().isEmpty()) {
          // TODO: multiple ID columns
          v = new NamedRealVector(v, tokens[inboundSettings.getIdColumns().iterator().next()]);
        }
        // Assign the vector to a fold
        int fold = rand.nextInt(crossfold.getNumFolds());
        ret.get(fold).add(v);
      }
    }
    return ret;
  }

  private static double asDouble(String token) {
    try {
      return Double.valueOf(token);
    } catch (NumberFormatException e) {
      log.warn("Invalid numeric token: {}", token);
      return Double.NaN;
    }
  }
}
