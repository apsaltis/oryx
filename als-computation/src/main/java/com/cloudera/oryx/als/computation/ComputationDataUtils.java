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

package com.cloudera.oryx.als.computation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.oryx.als.computation.types.ALSTypes;
import com.cloudera.oryx.als.computation.types.MatrixRow;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.collection.LongObjectMap;

/**
 * Provides utility methods used by the ALS Computation Layer's MapReduce jobs. These are generally methods
 * to side-load data from HDFS into memory.
 *
 * @author Sean Owen
 */
public final class ComputationDataUtils {

  private static final Logger log = LoggerFactory.getLogger(ComputationDataUtils.class);

  private static final Pattern PART_FILE_NAME_PATTERN = Pattern.compile("part-r-(\\d+)");

  private ComputationDataUtils() {
  }
  
  public static LongObjectMap<float[]> loadPartialY(int partition,
                                                  int numPartitions,
                                                  String prefix,
                                                  Configuration conf) throws IOException {
    LongObjectMap<float[]> result = new LongObjectMap<float[]>();
    AvroFileSource<MatrixRow> records = new AvroFileSource<MatrixRow>(
        Namespaces.toPath(prefix),
        (AvroType<MatrixRow>) ALSTypes.DENSE_ROW_MATRIX);
    for (MatrixRow record : records.read(conf)) {
      long id = record.getRowId();
      if (LongMath.mod(id, numPartitions) == partition) {
        result.put(id, record.getValues());
      }
    }
    return result;
  }

  public static LongSet readExpectedIDsFromPartition(int currentPartition,
                                                       int numPartitions,
                                                       String partitionsPrefix,
                                                       Progressable progressable,
                                                       Configuration conf) throws IOException {

    Store store = Store.get();
    if (!store.exists(partitionsPrefix, false)) {
      log.info("No IDs in {}, assuming all are allowed", partitionsPrefix);
      return null;
    }

    List<String> partitionFileKeys = store.list(partitionsPrefix, true);
    Preconditions.checkState(partitionFileKeys.size() == numPartitions,
                             "Number of partitions doesn't match number of ID files (%s vs %s). " +
                                 "Was the number of reducers changed?", partitionFileKeys.size(), numPartitions);

    // Shuffle order that the many reducers read the many files
    Collections.shuffle(partitionFileKeys);

    for (String partitionFileKey : partitionFileKeys) {
      Matcher m = PART_FILE_NAME_PATTERN.matcher(partitionFileKey);
      Preconditions.checkState(m.find(), "Bad part path file name: {}", partitionFileKey);
      int partPartition = Integer.parseInt(m.group(1));
      if (currentPartition == partPartition) {
        return readExpectedIDs(partitionFileKey, progressable, conf);
      }
    }
    throw new IllegalStateException("No file found for partition " + currentPartition);
  }

  private static LongSet readExpectedIDs(String key,
                                           Progressable progressable,
                                           Configuration conf) throws IOException {
    LongSet ids = new LongSet();
    long count = 0;
    for (long id : new AvroFileSource<Long>(Namespaces.toPath(key), Avros.longs()).read(conf)) {
      ids.add(id);
      if (++count % 10000 == 0) {
        progressable.progress();
      }
    }
    log.info("Read {} IDs from {}", ids.size(), key);
    return ids;
  }

}
