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

package com.cloudera.oryx.als.computation.known;

import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.computation.common.fn.OryxMapFn;

import com.google.common.collect.Lists;
import org.apache.crunch.Pair;

import java.util.Collection;

public final class CollectKnownItemsFn extends OryxMapFn<Pair<Long, LongFloatMap>, String> {
  @Override
  public String map(Pair<Long, LongFloatMap> input) {
    return input.first().toString() + '\t' + setToString(input.second());
  }

  private static String setToString(LongFloatMap map) {
    LongPrimitiveIterator it = map.keySetIterator();
    Collection<String> keyStrings = Lists.newArrayListWithCapacity(map.size());
    while (it.hasNext()) {
      keyStrings.add(Long.toString(it.nextLong()));
    }
    return DelimitedDataUtils.encode(keyStrings);
  }
}
