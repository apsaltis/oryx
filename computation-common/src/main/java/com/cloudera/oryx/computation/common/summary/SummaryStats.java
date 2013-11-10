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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public final class SummaryStats implements Serializable {

  private String name;
  private Numeric numeric;
  private SortedMap<String, Entry> histogram;
  private Boolean trimmed;

  // For Jackson serialization
  private SummaryStats() { }
  
  public SummaryStats(String name) {
    this.name = name;
    this.numeric = null;
    this.histogram = null;
  }
  
  public SummaryStats(String name, Numeric numeric) {
    this.name = name;
    this.numeric = Preconditions.checkNotNull(numeric);
    this.histogram = null;
  }
  
  public SummaryStats(String name, Map<String, Entry> histogram, boolean trimmed) {
    this.name = name;
    this.numeric = null;
    this.histogram = Maps.newTreeMap();
    if (histogram != null) {
      this.histogram.putAll(histogram);
    }
    if (trimmed) {
      this.trimmed = Boolean.TRUE;
    }
  }
  
  public boolean isEmpty() {
    return numeric == null && histogram == null;
  }
  
  public boolean isNumeric() {
    return numeric != null;
  }
  
  public String getName() {
    return name;
  }

  public double mean() {
    return numeric == null ? Double.NaN : numeric.mean();
  }
  
  public double stdDev() {
    return numeric == null ? Double.NaN : numeric.stdDev();
  }
  
  public double range() {
    return numeric == null ? Double.NaN : numeric.range();
  }
  
  public double min() {
    return numeric == null ? Double.NaN : numeric.min();
  }
  
  public double max() {
    return numeric == null ? Double.NaN : numeric.max();
  }
  
  public long getMissing() {
    return numeric == null ? 0L : numeric.getMissing();
  }
  
  public boolean isTrimmed() {
    return trimmed != null && trimmed;
  }
  
  public List<String> getLevels() {
    if (histogram == null) {
      return ImmutableList.of();
    }
    List<String> levels = Lists.newArrayList(histogram.keySet());
    Collections.sort(levels);
    return levels;
  }
  
  public int numLevels() {
    return histogram == null ? 1 : histogram.size();
  }
  
  public int index(String value) {
    if (histogram == null) {
      return -1;
    }
    Entry e = histogram.get(value);
    if (e == null) {
      return -1;
    } else {
      return histogram.headMap(value).size();
    }
  }  
}