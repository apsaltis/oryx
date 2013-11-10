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

package com.cloudera.oryx.als.computation.recommend;

import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class KnownItemsFn extends OryxDoFn<String, Pair<Long, LongSet>> {

  private String separator;

  @Override
  public void initialize() {
    super.initialize();
    separator = "\t"; //TODO
  }

  @Override
  public void process(String line, Emitter<Pair<Long, LongSet>> emitter) {
    int dividedAt = line.indexOf(separator);
    emitter.emit(Pair.of(Long.parseLong(line.substring(0, dividedAt)), stringToSet(line.substring(dividedAt + 1))));
  }

  private static LongSet stringToSet(CharSequence values) {
    LongSet result = new LongSet();
    for (String valueString : DelimitedDataUtils.decode(values)) {
      result.add(Long.parseLong(valueString));
    }
    return result;
  }
}
