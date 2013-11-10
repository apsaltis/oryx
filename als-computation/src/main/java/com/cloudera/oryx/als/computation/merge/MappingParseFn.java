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

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;

public final class MappingParseFn extends OryxDoFn<String, Pair<Long, String>> {

  @Override
  public void process(String line, Emitter<Pair<Long, String>> emitter) {
    String[] columns = DelimitedDataUtils.decode(line);

    String userID = columns[0];
    String itemID = columns[1];
    long numericUserID = StringLongMapping.toLong(userID);
    long numericItemID = StringLongMapping.toLong(itemID);

    if (!Long.toString(numericUserID).equals(userID)) {
      emitter.emit(Pair.of(numericUserID, userID));
    }
    if (!Long.toString(numericItemID).equals(itemID)) {
      emitter.emit(Pair.of(numericItemID, itemID));
    }
  }

}
