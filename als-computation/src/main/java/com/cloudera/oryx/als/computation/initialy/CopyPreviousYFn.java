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

package com.cloudera.oryx.als.computation.initialy;

import com.cloudera.oryx.als.common.DataUtils;
import com.cloudera.oryx.computation.common.fn.OryxMapFn;
import com.google.common.base.Preconditions;
import org.apache.crunch.Pair;

public final class CopyPreviousYFn extends OryxMapFn<String, Pair<Long, float[]>> {

  @Override
  public Pair<Long, float[]> map(String line) {
    int tab = line.indexOf('\t');
    Preconditions.checkArgument(tab > 0, "Line must have a tab delimiter");
    long itemID = Long.parseLong(line.substring(0, tab));
    float[] featureVector = DataUtils.readFeatureVector(line.substring(tab + 1));
    return Pair.of(itemID, featureVector);
  }
}
