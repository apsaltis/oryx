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

package com.cloudera.oryx.computation.common.fn;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.csv.CSVRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;

import com.cloudera.oryx.computation.common.types.MLRecords;

public final class StringSplitFn extends DoFn<String, Record> {

  public static PCollection<Record> apply(PCollection<String> in) {
    return in.parallelDo("string-split",
        new StringSplitFn(),
        MLRecords.csvRecord(in.getTypeFamily(), String.valueOf(DelimitedDataUtils.DELIMITER)));
  }

  @Override
  public void process(String line, Emitter<Record> emitter) {
    emitter.emit(new CSVRecord(DelimitedDataUtils.decode(line)));
  }

}
