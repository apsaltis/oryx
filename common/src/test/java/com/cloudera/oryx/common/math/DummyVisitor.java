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

package com.cloudera.oryx.common.math;

import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Pair;

import java.util.List;

final class DummyVisitor extends AbstractRealVectorPreservingVisitor {

  private final List<Pair<Integer,Double>> seenValues = Lists.newArrayList();

  @Override
  public void visit(int index, double value) {
    seenValues.add(new Pair<Integer, Double>(index, value));
  }

  List<Pair<Integer,Double>> getSeenValues() {
    return seenValues;
  }

}
