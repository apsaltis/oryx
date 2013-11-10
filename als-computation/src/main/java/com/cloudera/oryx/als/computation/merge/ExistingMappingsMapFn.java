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

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;

public final class ExistingMappingsMapFn extends OryxDoFn<String, Pair<Long, String>> {

  @Override
  public void process(String line, Emitter<Pair<Long, String>> emitter) {
    String[] columns = DelimitedDataUtils.decode(line);
    long numericID = Long.parseLong(columns[0]);
    String id = columns[1];
    emitter.emit(Pair.of(numericID, id));
  }

}
