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

package com.cloudera.oryx.computation.common.types;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Utility methods for reading and writing {@link Serializable} objects from/to Avro.
 */
public final class Serializables {

  public static <T extends Serializable> AvroType<T> avro(Class<T> clazz) {
    return (AvroType<T>) ptype(clazz, AvroTypeFamily.getInstance());
  }

  public static <T extends Serializable> PType<T> writable(Class<T> clazz) {
    return ptype(clazz, WritableTypeFamily.getInstance());
  }

  public static <T extends Serializable> PType<T> ptype(Class<T> clazz, PTypeFamily ptf) {
    return ptf.derived(clazz,
        new MapFn<ByteBuffer, T>() {

          @Override
          public T map(ByteBuffer input) {
            ByteArrayInputStream bais = new ByteArrayInputStream(input.array(), input.position(), input.limit());
            try {
              ObjectInputStream ois = new ObjectInputStream(bais);
              try {
                @SuppressWarnings("unchecked")
                T ret = (T) ois.readObject();
                return ret;
              } finally {
                ois.close();
              }
            } catch (ClassNotFoundException cnfe) {
              throw new CrunchRuntimeException(cnfe);
            } catch (IOException ioe) {
              throw new CrunchRuntimeException(ioe);
            }
          }
        },
        new MapFn<T, ByteBuffer>() {
          @Override
          public ByteBuffer map(T input) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
              ObjectOutputStream oos = new ObjectOutputStream(baos);
              try {
                oos.writeObject(input);
              } finally {
                oos.close();
              }
            } catch (IOException ioe) {
              throw new CrunchRuntimeException(ioe);
            }
            return ByteBuffer.wrap(baos.toByteArray());
          }
        },
        ptf.bytes());
  }

  private Serializables() {}
}
