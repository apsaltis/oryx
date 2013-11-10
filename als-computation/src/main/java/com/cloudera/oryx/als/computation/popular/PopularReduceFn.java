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

package com.cloudera.oryx.als.computation.popular;

import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import com.google.common.base.Preconditions;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class PopularReduceFn extends OryxReduceDoFn<Integer, Iterable<LongSet>, Long> {

  @Override
  public void process(Pair<Integer, Iterable<LongSet>> input, Emitter<Long> emitter) {
    Preconditions.checkState(input.first() == getPartition(),
                             "Key must match partition: %s != %s", input.first(), getPartition());
    for (LongSet set : input.second()) {
      LongPrimitiveIterator it = set.iterator();
      while (it.hasNext()) {
        emitter.emit(it.nextLong());
      }
    }
  }
}
