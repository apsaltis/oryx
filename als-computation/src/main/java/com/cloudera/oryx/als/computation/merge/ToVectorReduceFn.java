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

package com.cloudera.oryx.als.computation.merge;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class ToVectorReduceFn extends OryxReduceDoFn<Long, Iterable<NumericIDValue>, Pair<Long, LongFloatMap>> {
  @Override
  public void process(Pair<Long, Iterable<NumericIDValue>> input, Emitter<Pair<Long, LongFloatMap>> emitter) {
    LongFloatMap map = new LongFloatMap();
    for (NumericIDValue value : input.second()) {
      map.put(value.getID(), value.getValue());
    }
    if (!map.isEmpty()) {
      emitter.emit(Pair.of(input.first(), map));
    }
  }
}
