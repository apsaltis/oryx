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

package com.cloudera.oryx.rdf.serving.generation;

import com.google.common.collect.BiMap;

import java.util.Map;

import com.cloudera.oryx.rdf.common.tree.DecisionForest;

/**
 * @author Sean Owen
 */
public final class Generation {

  private final DecisionForest forest;
  private final Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping;

  public Generation(DecisionForest forest, Map<Integer, BiMap<String, Integer>> columnToCategoryNameToIDMapping) {
    this.forest = forest;
    this.columnToCategoryNameToIDMapping = columnToCategoryNameToIDMapping;
  }

  public DecisionForest getForest() {
    return forest;
  }

  public Map<Integer, BiMap<String, Integer>> getColumnToCategoryNameToIDMapping() {
    return columnToCategoryNameToIDMapping;
  }

}
