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

package com.cloudera.oryx.computation.common.summary;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.cloudera.oryx.computation.common.json.JacksonUtils;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.Spec;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.materialize.pobject.PObjectImpl;
import org.apache.crunch.types.avro.Avros;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Summarizer {
  
  private static final Logger LOG = LoggerFactory.getLogger(Summarizer.class);
  
  private final Set<Integer> ignoredColumns = Sets.newHashSet();
  private final Set<Integer> categoricalColumns = Sets.newHashSet();
  private Spec spec = null;

  public Summarizer spec(Spec spec) {
    this.spec = spec;
    return this;
  }
  
  public Summarizer ignoreColumns(Integer... columns) {
    return ignoreColumns(Arrays.asList(columns));
  }

  public Summarizer ignoreColumns(Iterable<Integer> columns) {
    for (Integer c : columns) {
      ignoredColumns.add(c);
    }
    return this;
  }

  public Summarizer categoricalColumns(Integer... columns) {
    return categoricalColumns(Arrays.asList(columns));
  }

  public Summarizer categoricalColumns(Iterable<Integer> columns) {
    for (Integer c : columns) {
      categoricalColumns.add(c);
    }
    return this;
  }

  public PObject<Summary> build(PCollection<Record> input) {
    return new SummaryPObject(spec, buildInternal(input));
  }

  public PCollection<String> buildJson(PCollection<Record> input) {
    return buildInternal(input).parallelDo("to-json", new ToJsonFn(spec), input.getTypeFamily().strings());
  }

  private PTable<Integer, Pair<Long, InternalStats>> buildInternal(PCollection<Record> records) {
    return records.parallelDo("summarize",
        new SummarizeFn(ignoredColumns, categoricalColumns),
        Avros.tableOf(Avros.ints(), Avros.pairs(Avros.longs(), Avros.reflects(InternalStats.class))))
        .groupByKey(1)
        .combineValues(
            Aggregators.pairAggregator(Aggregators.SUM_LONGS(),
                new InternalStats.InternalStatsAggregator()));
  }

  private static Summary toSummary(Spec spec, Iterable<Pair<Integer, Pair<Long, InternalStats>>> iter) {
    List<SummaryStats> ss = Lists.newArrayList();
    int fieldCount = 0;
    long recordCount = 0L;
    for (Pair<Integer, Pair<Long, InternalStats>> p : iter) {
      fieldCount++;
      recordCount = p.second().first();
      String name = spec != null ? spec.getField(p.first()).name() : "c" + p.first();
      SummaryStats stats = p.second().second().toSummaryStats(name, recordCount);
      if (stats.getMissing() > 0) {
        LOG.warn("{} missing/invalid values for numeric field {}, named '{}'",
            stats.getMissing(), p.first(), name);
      }
      while (ss.size() <= p.first()) {
        ss.add(null); // fill in any blanks
      }
      ss.set(p.first(), stats);
    }
    if (spec != null) {
      while (ss.size() < spec.size()) {
        ss.add(null); // Add blanks at the end
      }
      // Add placeholders for ignored fields in the summary
      for (int i = 0; i < spec.size(); i++) {
        SummaryStats s = ss.get(i);
        if (s == null) {
          String name = spec.getField(i).name();
          ss.set(i, new SummaryStats(name));
        }
      }
    }
    return new Summary(recordCount, fieldCount, ss);
  }

  private static final class SummaryPObject extends PObjectImpl<Pair<Integer, Pair<Long, InternalStats>>, Summary> {
    private final Spec spec;
    
    private SummaryPObject(Spec spec, PCollection<Pair<Integer, Pair<Long, InternalStats>>> pc) {
      super(pc);
      this.spec = spec;
    }
    
    @Override
    protected Summary process(Iterable<Pair<Integer, Pair<Long, InternalStats>>> iter) {
      return toSummary(spec, iter);
    }
  }

  private static final class ToJsonFn extends OryxDoFn<Pair<Integer, Pair<Long, InternalStats>>, String> {

    private final Spec spec;
    private final Collection<Pair<Integer, Pair<Long, InternalStats>>> values;

    ToJsonFn(Spec spec) {
      this.spec = spec;
      values = Lists.newArrayList();
    }

    @Override
    public void process(Pair<Integer, Pair<Long, InternalStats>> input, Emitter<String> emitter) {
      values.add(input);
    }

    @Override
    public void cleanup(Emitter<String> emitter) {
      Summary summary = toSummary(spec, values);
      ObjectMapper mapper = JacksonUtils.getObjectMapper();
      try {
        emitter.emit(mapper.writeValueAsString(summary));
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  private static final class SummarizeFn extends OryxDoFn<Record, Pair<Integer, Pair<Long, InternalStats>>> {
    private final Collection<Integer> ignoredColumns;
    private final Collection<Integer> categoricalColumns;
    private final Map<Integer, InternalStats> stats;
    private long count;
    
    private SummarizeFn(Collection<Integer> ignoreColumns,
                        Collection<Integer> categoricalColumns) {
      this.ignoredColumns = ignoreColumns;
      this.categoricalColumns = categoricalColumns;
      this.stats = Maps.newHashMap();
      this.count = 0;
    }
    
    @Override
    public void process(Record record,
        Emitter<Pair<Integer, Pair<Long, InternalStats>>> emitter) {
      for (int idx = 0; idx < record.getSpec().size(); idx++) {
        if (!ignoredColumns.contains(idx)) {
          InternalStats ss = stats.get(idx);
          if (ss == null) {
            ss = new InternalStats();
            stats.put(idx, ss);
          }
          if (categoricalColumns.contains(idx)) {
            ss.addCategorical(record.getAsString(idx));
          } else {
            ss.addNumeric(record.getAsDouble(idx));
          }
        }
      }
      count++;
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Pair<Long, InternalStats>>> emitter) {
      for (Map.Entry<Integer, InternalStats> e : stats.entrySet()) {
        emitter.emit(Pair.of(e.getKey(), Pair.of(count, e.getValue())));
      }
      stats.clear();
    }
  }
}
