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

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.computation.common.fn.StringSplitFn;
import com.cloudera.oryx.computation.common.records.Record;

import com.cloudera.oryx.kmeans.computation.MLAvros;

import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.common.math.Vectors;
import com.cloudera.oryx.computation.common.summary.Summarizer;
import com.google.common.collect.ImmutableList;

public final class StringParsingTest extends OryxTest {

  @Test
  public void testSimple() {
    PCollection<String> input = MemPipeline.typedCollectionOf(
        Avros.strings(),
        "1.0,2.0,3.0",
        "0.4,2.0,1.0",
        "3.2,17.0,29.0");
    PCollection<Record> elems = StringSplitFn.apply(input);
    PCollection<RealVector> vecs = elems.parallelDo(new StandardizeFn(), MLAvros.vector());
    assertEquals(ImmutableList.of(Vectors.of(1, 2, 3), Vectors.of(0.4, 2, 1),
        Vectors.of(3.2, 17, 29)), vecs.materialize());
  }
  
  @Test
  public void testQuoted() {
    PCollection<String> input = MemPipeline.typedCollectionOf(
        Avros.strings(),
        "1.0,\"a,b,c\",3.0,y",
        "0.4,\"b,q\",1.0,x",
        "3.2,\"c\",29.0,z");
    PCollection<Record> elems = StringSplitFn.apply(input);
    for (Record r : elems.materialize()) {
      assertEquals(4, r.size());
    }
  }
  
  @Test
  public void testCategorical() {
    PCollection<String> input = MemPipeline.typedCollectionOf(
        Avros.strings(),
        "1.0,a,3.0,y",
        "0.4,b,1.0,x",
        "3.2,c,29.0,z");
    PCollection<Record> elems = StringSplitFn.apply(input);
    Summary s = new Summarizer()
      .categoricalColumns(1, 3)
      .build(elems).getValue();
    PCollection<RealVector> vecs = elems.parallelDo(new StandardizeFn(s), MLAvros.vector());
    assertEquals(ImmutableList.of(
        Vectors.of(1.0, 1, 0, 0, 3.0, 0.0, 1.0, 0.0),
        Vectors.of(0.4, 0, 1, 0, 1.0, 1.0, 0.0, 0.0),
        Vectors.of(3.2, 0, 0, 1, 29.0, 0, 0, 1)),
        vecs.materialize());
  }

}
