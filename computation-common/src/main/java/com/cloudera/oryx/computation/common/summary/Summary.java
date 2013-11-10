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
import java.util.List;
import java.util.Map;

import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.RecordSpec;
import com.cloudera.oryx.computation.common.records.Spec;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public final class Summary implements Serializable {

  private List<SummaryStats> stats = Lists.newArrayList();
  private long recordCount;
  private int fieldCount;

  private transient Spec spec;

  public Summary() {}
  
  public Summary(long recordCount, int fieldCount, List<SummaryStats> stats) {
    this.recordCount = recordCount;
    this.fieldCount = fieldCount;
    this.stats = stats;
    this.spec = createSpec(stats);
  }
  
  public Summary(Spec spec, List<SummaryStats> stats) {
    this.spec = spec;
    this.stats = stats;
  }
  
  public Spec getSpec() {
    if (spec == null) {
      spec = createSpec(stats);
    }
    return spec;
  }
  
  private static Spec createSpec(List<SummaryStats> stats) {
    RecordSpec.Builder rsb = RecordSpec.builder();
    for (int i = 0; i < stats.size(); i++) {
      SummaryStats ss = stats.get(i);
      if (ss != null) {
        String field = ss.getName();
        if (field == null) {
          field = "c" + i;
        }
        if (ss.isNumeric()) {
          rsb.add(field, DataType.DOUBLE);
        } else {
          rsb.add(field, DataType.STRING);
        }
      } else {
        rsb.add("c" + i, DataType.STRING);
      }
    }
    return rsb.build();
  }

  public Map<Integer, BiMap<String, Integer>> getCategoryLevelsMapping() {
    Map<Integer, BiMap<String, Integer>> levelsMap = Maps.newHashMap();
    for (int i = 0; i < stats.size(); i++) {
      if (stats.get(i) != null && !stats.get(i).isNumeric()) {
        List<String> levels = stats.get(i).getLevels();
        BiMap<String, Integer> bm = HashBiMap.create(levels.size());
        for (int j = 0; j < levels.size(); j++) {
          bm.put(levels.get(j), j);
        }
        levelsMap.put(i, bm);
      }
    }
    return levelsMap;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public int getFieldCount() {
    return fieldCount;
  }
  
  public SummaryStats getStats(int field) {
    if (field >= stats.size()) {
      return null;
    }
    return stats.get(field);
  }
  
  public int getNetLevels() {
    int netLevels = 0;
    for (SummaryStats ss : stats) {
      if (ss != null && !ss.isEmpty()) {
        netLevels += ss.numLevels() - 1;
      }
    }
    return netLevels;
  }
}
