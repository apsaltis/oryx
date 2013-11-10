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

package com.cloudera.oryx.kmeans.computation;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public final class AvroUtils {

  private static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);

  public static <T extends Serializable> T readSerialized(String key, Configuration conf) throws IOException {
    String firstFileKey = Store.get().list(key, true).get(0);
    FsInput fsInput = new FsInput(Namespaces.toPath(firstFileKey), conf);
    DatumReader<ByteBuffer> dr = new GenericDatumReader<ByteBuffer>(BYTES_SCHEMA);
    DataFileReader<ByteBuffer> dfr = new DataFileReader<ByteBuffer>(fsInput, dr);
    ByteBuffer bytes = dfr.next();
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes.array(), bytes.position(), bytes.limit());
    ObjectInputStream ois = new ObjectInputStream(bais);
    try {
      return (T) ois.readObject();
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalStateException(cnfe);
    } finally {
      ois.close();
    }
  }

  private AvroUtils() {}
}
