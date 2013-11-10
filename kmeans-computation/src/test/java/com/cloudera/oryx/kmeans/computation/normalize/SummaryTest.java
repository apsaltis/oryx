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

package com.cloudera.oryx.kmeans.computation.normalize;

import java.util.Arrays;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.Spec;
import com.cloudera.oryx.computation.common.records.vectors.VectorRecord;

import com.cloudera.oryx.kmeans.computation.MLAvros;

import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.RecordSpec;
import com.cloudera.oryx.computation.common.records.csv.CSVRecord;
import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.common.math.Vectors;
import com.cloudera.oryx.computation.common.summary.Summarizer;
import com.cloudera.oryx.computation.common.types.MLRecords;
import com.google.common.collect.ImmutableList;

public final class SummaryTest extends OryxTest {

  private static final PCollection<RealVector> VECS = MemPipeline.typedCollectionOf(
      MLAvros.vector(),
      Vectors.of(1.0, 3.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(3.0, 1.0),
      Vectors.of(3.0, 3.0));
  
  private static final PCollection<String> STRINGS = MemPipeline.typedCollectionOf(
      Avros.strings(),
      "1.0,NA,2.0",
      "1.0,2.0,3.0");

  @Test
  public void testZScores() {
    PCollection<Record> elems = VECS.parallelDo(new MapFn<RealVector, Record>() {
      @Override
      public Record map(RealVector vec) {
        return new VectorRecord(vec);
      }
    }, null);
    Summarizer sr = new Summarizer();
    Summary s = sr.build(elems).getValue();
    StandardizeFn fn = new StandardizeFn(s, Transform.Z);
    assertEquals(ImmutableList.of(Vectors.of(-1, 1),
        Vectors.of(-1, -1), Vectors.of(1, -1),
        Vectors.of(1, 1)), elems.parallelDo(fn, MLAvros.vector()).materialize());
  }
  
  @Test
  public void testMissing() throws Exception {
    PCollection<Record> elems = STRINGS.parallelDo(new MapFn<String, Record>() {
      @Override
      public Record map(String input) {
        return new CSVRecord(Arrays.asList(input.split(",")));
      }
    }, MLRecords.csvRecord(AvroTypeFamily.getInstance(), ","));
    Summarizer sr = new Summarizer();
    Summary s = sr.build(elems).getValue();
    assertEquals(1, s.getStats(1).getMissing());
    assertEquals(2.0, s.getStats(1).mean(), 0.01);
    assertEquals(0.0, s.getStats(1).stdDev(), 0.01);
  }
  
  @Test
  public void testTrailingIgnoredFields() throws Exception {
    Spec spec = RecordSpec.builder().add("field1", DataType.DOUBLE)
        .add("field2", DataType.DOUBLE).add("field3", DataType.DOUBLE).build();
    PCollection<Record> elems = STRINGS.parallelDo(new MapFn<String, Record>() {
      @Override
      public Record map(String input) {
        return new CSVRecord(Arrays.asList(input.split(",")));
      }
    }, MLRecords.csvRecord(AvroTypeFamily.getInstance(), ","));
    Summarizer sr = new Summarizer().spec(spec).ignoreColumns(2);
    sr.build(elems).getValue();
  }
}
