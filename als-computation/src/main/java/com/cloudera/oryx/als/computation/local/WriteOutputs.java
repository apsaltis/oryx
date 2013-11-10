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

package com.cloudera.oryx.als.computation.local;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.common.pmml.ALSModelDescription;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.io.DelimitedDataUtils;

final class WriteOutputs implements Callable<Object> {

  private static final Logger log = LoggerFactory.getLogger(WriteOutputs.class);

  private static final char KEY_VALUE_DELIMITER = '\t'; // Matches Hadoop TextOutputFormat
  private static final String SINGLE_OUT_FILENAME = "0.csv.gz";

  private final File modelDir;
  private final LongObjectMap<LongFloatMap> RbyRow;
  private final LongObjectMap<LongSet> knownItemIDs;
  private final LongObjectMap<float[]> X;
  private final LongObjectMap<float[]> Y;
  private final StringLongMapping idMapping;

  WriteOutputs(File modelDir,
               LongObjectMap<LongFloatMap> RbyRow,
               LongObjectMap<LongSet> knownItemIDs,
               LongObjectMap<float[]> X,
               LongObjectMap<float[]> Y,
               StringLongMapping idMapping) {
    this.modelDir = modelDir;
    this.RbyRow = RbyRow;
    this.knownItemIDs = knownItemIDs;
    this.X = X;
    this.Y = Y;
    this.idMapping = idMapping;
  }

  @Override
  public Void call() throws IOException {
    log.info("Writing current input");
    writeCombinedInput(RbyRow, new File(modelDir, "input"));
    log.info("Writing known items");
    writeIDIDsMap(knownItemIDs, new File(modelDir, "knownItems"));
    log.info("Writing X");
    writeIDFloatMap(X, new File(modelDir, "X"));
    log.info("Writing Y");
    writeIDFloatMap(Y, new File(modelDir, "Y"));
    log.info("Writing ID mapping");
    writeMapping(idMapping, new File(modelDir, "idMapping"));
    log.info("Writing model");
    File modelDescriptionFile = new File(modelDir, "model.pmml.gz");
    ALSModelDescription modelDescription = new ALSModelDescription();
    modelDescription.setKnownItemsPath("knownItems");
    modelDescription.setXPath("X");
    modelDescription.setYPath("Y");
    modelDescription.setIDMappingPath("idMapping");
    ALSModelDescription.write(modelDescriptionFile, modelDescription);
    return null;
  }

  private static void writeCombinedInput(LongObjectMap<LongFloatMap> RbyRow, File inputDir) throws IOException {
    File outFile = new File(inputDir, SINGLE_OUT_FILENAME);
    Files.createParentDirs(outFile);
    Writer out = IOUtils.buildGZIPWriter(outFile);
    try {
      for (LongObjectMap.MapEntry<LongFloatMap> row : RbyRow.entrySet()) {
        long rowID = row.getKey();
        for (LongFloatMap.MapEntry entry : row.getValue().entrySet()) {
          long colID = entry.getKey();
          float value = entry.getValue();
          out.write(DelimitedDataUtils.encode(Long.toString(rowID), Long.toString(colID), Float.toString(value)));
          out.write('\n');
        }
      }
    } finally {
      out.close();
    }
  }

  private static void writeIDIDsMap(LongObjectMap<LongSet> idIDs, File idIDsDir) throws IOException {
    if (idIDs.isEmpty()) {
      return;
    }
    File outFile = new File(idIDsDir, SINGLE_OUT_FILENAME);
    Files.createParentDirs(outFile);
    Writer out = IOUtils.buildGZIPWriter(outFile);
    try {
      for (LongObjectMap.MapEntry<LongSet> entry : idIDs.entrySet()) {
        out.write(String.valueOf(entry.getKey()));
        out.write(KEY_VALUE_DELIMITER);
        LongSet ids = entry.getValue();
        LongPrimitiveIterator it = ids.iterator();
        Collection<String> keyStrings = Lists.newArrayListWithCapacity(ids.size());
        while (it.hasNext()) {
          keyStrings.add(Long.toString(it.nextLong()));
        }
        out.write(DelimitedDataUtils.encode(keyStrings));
        out.write('\n');
      }
    } finally {
      out.close();
    }
  }

  private static void writeIDFloatMap(LongObjectMap<float[]> idFloatMap, File idFloatDir) throws IOException {
    if (idFloatMap.isEmpty()) {
      return;
    }
    File outFile = new File(idFloatDir, SINGLE_OUT_FILENAME);
    Files.createParentDirs(outFile);
    Writer out = IOUtils.buildGZIPWriter(outFile);
    try {
      for (LongObjectMap.MapEntry<float[]> entry : idFloatMap.entrySet()) {
        out.write(String.valueOf(entry.getKey()));
        out.write(KEY_VALUE_DELIMITER);
        float[] f = entry.getValue();
        String[] floatStrings = new String[f.length];
        for (int i = 0; i < floatStrings.length; i++) {
          floatStrings[i] = Float.toString(f[i]);
        }
        out.write(DelimitedDataUtils.encode(floatStrings));
        out.write('\n');
      }
    } finally {
      out.close();
    }
  }

  private static void writeMapping(StringLongMapping idMapping, File idMappingDir) throws IOException {
    File outFile = new File(idMappingDir, SINGLE_OUT_FILENAME);
    Files.createParentDirs(outFile);
    Writer out = IOUtils.buildGZIPWriter(outFile);
    try {
      Lock lock = idMapping.getLock().readLock();
      lock.lock();
      try {
        for (LongObjectMap.MapEntry<String> entry : idMapping.getReverseMapping().entrySet()) {
          out.write(DelimitedDataUtils.encode(Long.toString(entry.getKey()), entry.getValue()));
          out.write('\n');
        }
      } finally {
        lock.unlock();
      }
    } finally {
      out.close();
    }
  }

}
