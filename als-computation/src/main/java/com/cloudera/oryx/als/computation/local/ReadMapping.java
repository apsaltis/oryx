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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.io.DelimitedDataUtils;

final class ReadMapping implements Callable<Object> {

  private static final Logger log = LoggerFactory.getLogger(ReadMapping.class);

  private final File mappingDir;
  private final StringLongMapping idMapping;

  ReadMapping(File mappingDir, StringLongMapping idMapping) {
    this.mappingDir = mappingDir;
    this.idMapping = idMapping;
  }

  @Override
  public Void call() throws IOException {
    File[] inputFiles = mappingDir.listFiles(IOUtils.CSV_COMPRESSED_FILTER);
    if (inputFiles == null) {
      return null;
    }
    for (File inputFile : inputFiles) {
      log.info("Reading {}", inputFile);
      for (CharSequence line : new FileLineIterable(inputFile)) {
        String[] columns = DelimitedDataUtils.decode(line);
        long numericID = Long.parseLong(columns[0]);
        String id = columns[1];
        idMapping.addMapping(id, numericID);
      }
    }
    return null;
  }

}
