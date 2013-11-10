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

import com.cloudera.oryx.common.math.NamedRealVector;
import com.cloudera.oryx.common.math.Vectors;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.cloudera.oryx.computation.common.json.JacksonUtils;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.vectors.VectorRecord;
import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.computation.common.summary.SummaryStats;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

final class StandardizeFn extends OryxDoFn<Record, RealVector> {

  private final String summaryKey;
  private final NormalizeSettings settings;
  private final Collection<Integer> ignoredColumns;
  private final Collection<Integer> idColumns;

  private Summary summary;
  private int expansion;
  private boolean sparse;

  StandardizeFn() {
    this(new NormalizeSettings(), ImmutableList.<Integer>of(), ImmutableList.<Integer>of(), "");
  }

  StandardizeFn(Summary summary) {
    this(new NormalizeSettings(), ImmutableList.<Integer>of(), ImmutableList.<Integer>of(), "");
    this.summary = summary;
  }

  StandardizeFn(Summary summary, Transform defaultTransform) {
    this(new NormalizeSettings(defaultTransform), ImmutableList.<Integer>of(), ImmutableList.<Integer>of(), "");
    this.summary = summary;
  }

  StandardizeFn(NormalizeSettings settings,
                Collection<Integer> ignoredColumns,
                Collection<Integer> idColumns,
                String summaryKey) {
    this.settings = settings;
    this.summaryKey = summaryKey;
    this.ignoredColumns = ignoredColumns;
    this.idColumns = idColumns;
  }

  @Override
  public void initialize() {
    super.initialize();
    if (summaryKey != null && !summaryKey.isEmpty()) {
      try {
        Store store = Store.get();
        List<String> pieces = store.list(summaryKey, true);
        if (pieces.isEmpty()) {
          // No summary file specified, which is okay for us
          summary = new Summary();
        } else if (pieces.size() > 1) {
          throw new IllegalStateException("Expected exactly one summary file: " + pieces);
        } else {
          try {
            summary = JacksonUtils.getObjectMapper().readValue(store.readFrom(pieces.get(0)), Summary.class);
          } catch (Exception e) {
            throw new CrunchRuntimeException(e);
          }
        }
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
    } else if (summary == null) {
      summary = new Summary();
    }

    //TODO: arbitrary ID columns
    this.expansion = -ignoredColumns.size() + summary.getNetLevels() -
        (!idColumns.isEmpty() && !ignoredColumns.contains(idColumns.iterator().next()) ? 1 : 0);
    this.sparse = settings.getSparse() != null ? settings.getSparse() :
        expansion > 2 * (summary.getFieldCount() - ignoredColumns.size());
  }

  @Override
  public void process(Record record, Emitter<RealVector> emitter) {
    int len = record.getSpec().size() + expansion;
    RealVector v;
    if (record instanceof VectorRecord) {
      RealVector innerRecord = ((VectorRecord) record).getVector();
      if (innerRecord instanceof ArrayRealVector) {
        v = Vectors.dense(len);
      } else {
        v = Vectors.sparse(len);
      }
    } else if (sparse) {
      v = Vectors.sparse(len);
    } else {
      v = Vectors.dense(len);
    }
    increment("NORMALIZE", "INPUT_RECORDS");

    int offset = 0;
    for (int i = 0; i < record.getSpec().size(); i++) {
      if (!idColumns.contains(i) && !ignoredColumns.contains(i)) {
        SummaryStats ss = summary.getStats(i);
        if (ss == null || ss.isEmpty()) {
          v.setEntry(offset, record.getAsDouble(i));
          offset++;
        } else if (ss.isNumeric()) {
          Transform t = settings.getTransform(i);
          double raw = record.getAsDouble(i);
          if (Double.isNaN(raw)) {
            increment("NORMALIZE", "NaN record value");
            return;
          }
          double n = t.apply(raw, ss) * settings.getScale(i);
          v.setEntry(offset, n);
          offset++;
        } else {
          int index = ss.index(record.getAsString(i));
          if (index < 0) {
            increment("NORMALIZE", "Negative record index");
            return;
          }
          v.setEntry(offset + index, settings.getScale(i));
          offset += ss.numLevels();
        }
      }
    }

    increment("NORMALIZE", "PRE_NAMING");
    if (!idColumns.isEmpty()) {
      // TODO: arbitrary properties
      v = new NamedRealVector(v, record.getAsString(idColumns.iterator().next()));
    }
    emitter.emit(v);
    increment("NORMALIZE", "OUTPUT_VECTORS");
  }
}
