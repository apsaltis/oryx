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

package com.cloudera.oryx.als.computation;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.servcomp.Store;

public final class IDMappingState {

  public static final String ID_MAPPING_KEY = "ID_MAPPING";

  private final StringLongMapping idMapping;

  public IDMappingState(Configuration conf) throws IOException {
    this.idMapping = readIDMapping(conf.get(ID_MAPPING_KEY));
  }

  public StringLongMapping getIDMapping() {
    return idMapping;
  }

  private static StringLongMapping readIDMapping(String prefix) throws IOException {
    Store store = Store.get();
    StringLongMapping idMapping = new StringLongMapping();
    for (String filePrefix : store.list(prefix, true)) {
      for (CharSequence line : new FileLineIterable(store.readFrom(filePrefix))) {
        String[] columns = DelimitedDataUtils.decode(line);
        long numericID = Long.parseLong(columns[0]);
        String id = columns[1];
        idMapping.addMapping(id, numericID);
      }
    }
    return idMapping;
  }

}
