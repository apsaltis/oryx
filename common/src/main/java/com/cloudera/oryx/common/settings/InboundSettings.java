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

package com.cloudera.oryx.common.settings;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Encapsulates global config settings under the {@code inbound} key.
 */
public final class InboundSettings implements Serializable {

  private final List<String> columnNames;
  private final Collection<Integer> idColumns;
  private final Collection<Integer> categoricalColumns;
  private final Collection<Integer> numericColumns;
  private final Collection<Integer> ignoredColumns;
  private final Integer targetColumn;

  public static InboundSettings create(Config config) {
    Config inbound = config.getConfig("inbound");

    List<String> columnNames = inbound.getStringList("column-names");

    Function<Object,Integer> lookup = new LookupFunction(columnNames);

    Iterable<Integer> allColumns = Collections2.transform(columnNames, lookup);

    Collection<Integer> idColumns;
    if (inbound.hasPath("id-columns")) {
      idColumns = ImmutableSet.copyOf(
          Collections2.transform(inbound.getAnyRefList("id-columns"), lookup));
    } else {
      idColumns = ImmutableSet.of();
    }

    Collection<Integer> ignoredColumns;
    if (inbound.hasPath("ignored-columns")) {
      ignoredColumns = ImmutableSet.copyOf(
          Collections2.transform(inbound.getAnyRefList("ignored-columns"), lookup));
    } else {
      ignoredColumns = ImmutableSet.of();
    }

    Collection<Integer> categoricalColumns;
    Collection<Integer> numericColumns;
    if (inbound.hasPath("categorical-columns")) {
      Preconditions.checkState(!inbound.hasPath("numeric-columns"));
      categoricalColumns = Sets.newHashSet(
          Collections2.transform(inbound.getAnyRefList("categorical-columns"), lookup));
      numericColumns = Sets.newHashSet(allColumns);
      numericColumns.removeAll(categoricalColumns);
    } else if (inbound.hasPath("numeric-columns")) {
      Preconditions.checkState(!inbound.hasPath("categorical-columns"));
      numericColumns = Sets.newHashSet(
          Collections2.transform(inbound.getAnyRefList("numeric-columns"), lookup));
      categoricalColumns = Sets.newHashSet(allColumns);
      categoricalColumns.removeAll(numericColumns);
    } else {
      throw new IllegalArgumentException("No categorical-columns or numeric-columns set");
    }
    numericColumns.removeAll(idColumns);
    numericColumns.removeAll(ignoredColumns);
    categoricalColumns.removeAll(idColumns);
    categoricalColumns.removeAll(ignoredColumns);

    Integer targetColumn = null;
    if (inbound.hasPath("target-column")) {
      targetColumn = lookup.apply(inbound.getAnyRef("target-column"));
      Preconditions.checkState(categoricalColumns.contains(targetColumn) ||
                               numericColumns.contains(targetColumn),
                               "Target column not specified as numeric or categorical");
    }

    return new InboundSettings(columnNames,
                               idColumns,
                               categoricalColumns,
                               numericColumns,
                               ignoredColumns,
                               targetColumn);
  }

  private static final class LookupFunction implements Function<Object, Integer> {
    private final List<String> columnNames;

    LookupFunction(List<String> columnNames) {
      this.columnNames = columnNames;
    }

    @Override
    public Integer apply(Object input) {
      if (input instanceof Integer) {
        return (Integer) input;
      }
      int index = columnNames.indexOf(input.toString());
      Preconditions.checkArgument(index >= 0, String.format("Could not find %s in list: %s", input, columnNames));
      return index;
    }
  }

  private InboundSettings(List<String> columnNames,
                          Collection<Integer> idColumns,
                          Collection<Integer> categoricalColumns,
                          Collection<Integer> numericColumns,
                          Collection<Integer> ignoredColumns,
                          Integer targetColumn) {
    this.columnNames = columnNames;
    this.idColumns = idColumns;
    this.categoricalColumns = categoricalColumns;
    this.numericColumns = numericColumns;
    this.ignoredColumns = ignoredColumns;
    this.targetColumn = targetColumn;
  }

  public Function<Object, Integer> getLookupFunction() {
    return new LookupFunction(columnNames);
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public Collection<Integer> getIdColumns() {
    return idColumns;
  }

  public Collection<Integer> getCategoricalColumns() {
    return categoricalColumns;
  }

  public Collection<Integer> getNumericColumns() {
    return numericColumns;
  }

  public Collection<Integer> getIgnoredColumns() {
    return ignoredColumns;
  }

  public boolean isIgnored(int col) {
    return ignoredColumns.contains(col) || idColumns.contains(col);
  }

  public boolean isNumeric(int index) {
    return numericColumns.contains(index);
  }

  public boolean isCategorical(int index) {
    return categoricalColumns.contains(index);
  }

  public Integer getTargetColumn() {
    return targetColumn;
  }

}
