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
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.LangUtils;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class DelimitedInputParseFn extends OryxDoFn<String, Pair<Long, NumericIDValue>> {

  @Override
  public void process(String line, Emitter<Pair<Long, NumericIDValue>> emitter) {
    String[] columns = DelimitedDataUtils.decode(line);

    long userID = StringLongMapping.toLong(columns[0]);
    long itemID = StringLongMapping.toLong(columns[1]);

    float pref;
    if (columns.length > 2) {
      String valueToken = columns[2];
      pref = valueToken.isEmpty() ? Float.NaN : LangUtils.parseFloat(valueToken);
    } else {
      pref = 1.0f;
    }

    emitter.emit(Pair.of(userID, new NumericIDValue(itemID, pref)));
  }

}
