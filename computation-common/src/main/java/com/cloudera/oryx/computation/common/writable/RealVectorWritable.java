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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.cloudera.oryx.computation.common.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.io.Varint;
import com.cloudera.oryx.common.math.AbstractRealVectorPreservingVisitor;
import com.cloudera.oryx.common.math.NamedRealVector;
import com.cloudera.oryx.common.math.SparseRealVector;
import com.cloudera.oryx.common.math.Vectors;

/**
 * This is a port of Mahout's {@code VectorWritable} and should be binary-compatible with its
 * serialization.
 *
 * @author Sean Owen
 * @author Mahout
 */
public final class RealVectorWritable implements Writable {

  private static final int FLAG_DENSE = 0x01;
  //private static final int FLAG_SEQUENTIAL = 0x02; // We don't have a sequential implementation
  private static final int FLAG_NAMED = 0x04;
  private static final int FLAG_LAX_PRECISION = 0x08;
  private static final int NUM_FLAGS = 4;

  private RealVector vector;

  /**
   * For serialization:
   */
  public RealVectorWritable() {
    this(null);
  }

  /**
   * @param vector vector to be written by this {@code RealVectorWritable}
   */
  public RealVectorWritable(RealVector vector) {
    this.vector = vector;
  }

  /**
   * @return {@link RealVector} that this is to write, or has just read
   */
  public RealVector get() {
    return vector;
  }

  public void set(RealVector vector) {
    Preconditions.checkNotNull(vector);
    this.vector = vector;
  }

  @Override
  public void write(final DataOutput out) throws IOException {

    boolean named = vector instanceof NamedRealVector;
    RealVector vectorToWrite = named ? ((NamedRealVector) vector).getDelegate() : vector;
    boolean dense = vectorToWrite instanceof ArrayRealVector; // Assuming this is the only dense implementation
    boolean sparse = vectorToWrite instanceof SparseRealVector;

    Preconditions.checkState(dense || sparse, "Unknown type of RealVector: " + vector.getClass());

    out.writeByte((dense ? FLAG_DENSE : 0) | (named ? FLAG_NAMED : 0) | FLAG_LAX_PRECISION);
    // sequential is always false, and not set
    // laxPrecision is always true

    Varint.writeUnsignedVarInt(vectorToWrite.getDimension(), out);

    if (dense) {
      vectorToWrite.walkInDefaultOrder(new AbstractRealVectorPreservingVisitor() {
        @Override
        public void visit(int index, double value) {
          try {
            out.writeFloat((float) value);
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        }
      });
    } else {
      SparseRealVector sparseVector = (SparseRealVector) vector;
      Varint.writeUnsignedVarInt(sparseVector.getNumEntries(), out);
      vectorToWrite.walkInOptimizedOrder(new AbstractRealVectorPreservingVisitor() {
        @Override
        public void visit(int index, double value) {
          try {
          Varint.writeUnsignedVarInt(index, out);
          out.writeFloat((float) value);
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        }
      });
    }

    if (named) {
      String name = ((NamedRealVector) this.vector).getName();
      out.writeUTF(name == null ? "" : name);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    int flags = in.readByte();
    Preconditions.checkArgument(flags >> NUM_FLAGS == 0, "Unknown flags set: %d", Integer.toString(flags, 2));

    boolean dense = (flags & FLAG_DENSE) != 0;
    // boolean sequential = (flags & FLAG_SEQUENTIAL) != 0; // Don't care
    boolean named = (flags & FLAG_NAMED) != 0;
    boolean laxPrecision = (flags & FLAG_LAX_PRECISION) != 0;

    int size = Varint.readUnsignedVarInt(in);
    RealVector v;
    if (dense) {
      double[] values = new double[size];
      for (int i = 0; i < size; i++) {
        values[i] = laxPrecision ? in.readFloat() : in.readDouble();
      }
      v = Vectors.of(values);
    } else {
      int numEntries = Varint.readUnsignedVarInt(in);
      v = Vectors.sparse(size, numEntries);
      for (int i = 0; i < numEntries; i++) {
        int index = Varint.readUnsignedVarInt(in);
        double value = laxPrecision ? in.readFloat() : in.readDouble();
        v.setEntry(index, value);
      }
    }

    if (named) {
      String name = in.readUTF();
      v = new NamedRealVector(v, name);
    }

    vector = v;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RealVectorWritable)) {
      return false;
    }
    RealVectorWritable other = (RealVectorWritable) o;
    return (vector == null && other.vector == null) || vector != null && vector.equals(other.vector);
  }

  @Override
  public int hashCode() {
    return vector == null ? 0 : vector.hashCode();
  }

  @Override
  public String toString() {
    return String.valueOf(vector);
  }

}
