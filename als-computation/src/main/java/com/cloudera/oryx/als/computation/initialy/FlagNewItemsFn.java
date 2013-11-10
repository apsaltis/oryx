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

import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.computation.common.fn.OryxMapFn;
import org.apache.crunch.Pair;

public final class FlagNewItemsFn extends OryxMapFn<Pair<Long, LongFloatMap>, Pair<Long, float[]>> {
  private static final float[] EMPTY = new float[0];

  @Override
  public Pair<Long, float[]> map(Pair<Long, LongFloatMap> input) {
    return Pair.of(input.first(), EMPTY);
  }
}
