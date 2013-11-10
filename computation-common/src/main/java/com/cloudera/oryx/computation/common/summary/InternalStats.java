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

import java.util.Map;
import java.util.Set;

import org.apache.crunch.fn.Aggregators.SimpleAggregator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public final class InternalStats {

  private static final int MAX_LEVELS = 1000000;

  public static final class InternalStatsAggregator extends SimpleAggregator<InternalStats> {
    private InternalStats agg;
    private final int maxLevels;

    public InternalStatsAggregator() {
      this(MAX_LEVELS);
    }

    public InternalStatsAggregator(int maxLevels) {
      this.maxLevels = maxLevels;
    }
    
    @Override
    public void reset() {
      agg = new InternalStats();
    }

    @Override
    public Iterable<InternalStats> results() {
      return ImmutableList.of(agg);
    }

    @Override
    public void update(InternalStats other) {
      agg.merge(other, maxLevels);
    }
  }
  
  private InternalNumeric internalNumeric;
  private Map<String, Entry> histogram;
  private boolean trimmed;
  
  public SummaryStats toSummaryStats(String name, long recordCount) {
    if (internalNumeric == null) {
      if (histogram == null) {
        return new SummaryStats(name);
      } else {
        return new SummaryStats(name, histogram, trimmed);
      }
    } else {
      return new SummaryStats(name, internalNumeric.toNumeric(recordCount));
    }
  }
  
  private InternalNumeric internalNumeric() {
    if (internalNumeric == null) {
      internalNumeric = new InternalNumeric();
    }
    return internalNumeric;
  }
  
  private Map<String, Entry> histogram() {
    if (histogram == null) {
      histogram = Maps.newHashMap();
    }
    return histogram;
  }

  public void addCategorical(String category) {
    addCategorical(category, MAX_LEVELS);
  }

  public void addCategorical(String category, int maxLevels) {
    Map<String, Entry> h = histogram();
    Entry entry = h.get(category);
    if (entry == null) {
      if (h.size() < maxLevels) {
        entry = new Entry();
        h.put(category, entry);
      } else {
        trimmed = true;
        return;
      }
    } 
    entry.inc();
  }
  
  public void addNumeric(double value) {
    internalNumeric().update(value);
  }
  
  public void merge(InternalStats other, int maxLevels) {
    if (other.internalNumeric != null) {
      internalNumeric().merge(other.internalNumeric);
    } else {
      Map<String, Entry> entries = histogram();
      Map<String, Entry> merged = Maps.newTreeMap();
      Set<String> keys = Sets.newTreeSet(
          Sets.union(entries.keySet(), other.histogram().keySet()));
      for (String key : keys) {
        Entry e = entries.get(key);
        Entry entry = other.histogram().get(key);
        Entry newEntry = new Entry();
        if (e != null) {
          newEntry.inc(e.getCount());
        }
        if (entry != null) {
          newEntry.inc(entry.getCount());
        }
        merged.put(key, newEntry);
        if (merged.size() == maxLevels) {
          this.trimmed = true;
          break;
        }
      }
      entries.clear();
      entries.putAll(merged);
      if (other.trimmed) {
        this.trimmed = true;
      }
    }
  }
}
