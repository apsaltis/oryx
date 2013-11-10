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

import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.computation.common.fn.OryxMapFn;
import org.apache.crunch.Pair;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public final class PopularMapFn extends OryxMapFn<Pair<Long, LongFloatMap>, Pair<Integer, LongSet>> {

  private static final Partitioner<Long,?> HASHER = new HashPartitioner<Long, Object>();

  private int numReducers;

  @Override
  public void initialize() {
    super.initialize();
    this.numReducers = getContext().getNumReduceTasks();
  }

  @Override
  public Pair<Integer, LongSet> map(Pair<Long, LongFloatMap> input) {
    LongFloatMap vector = input.second();
    LongSet targetIDs = new LongSet(vector.size());
    LongPrimitiveIterator it = vector.keySetIterator();
    while (it.hasNext()) {
      targetIDs.add(it.nextLong());
    }
    // Make sure we use exactly the same hash:
    int partition = HASHER.getPartition(input.first(), null, numReducers);
    return Pair.of(partition, targetIDs);
  }
}
