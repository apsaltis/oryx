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

package com.cloudera.oryx.kmeans.computation.evaluate;

import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.util.List;

public final class ReplicateValuesFn<V> extends OryxDoFn<V, Pair<Pair<Integer, Integer>, V>> {
  private final List<Integer> candidateKs;
  private final int replication;

  public ReplicateValuesFn(List<Integer> candidateKs, int replication) {
    this.candidateKs = candidateKs;
    this.replication = replication;
  }

  @Override
  public void process(V input, Emitter<Pair<Pair<Integer, Integer>, V>> emitter) {
    emitter.emit(Pair.of(Pair.of(1, 0), input)); // Default for K = 1
    for (Integer k : candidateKs) {
      if (k > 1) {
        for (int i = 0; i < replication; i++) {
          emitter.emit(Pair.of(Pair.of(k, i), input));
        }
      }
    }
  }
}
