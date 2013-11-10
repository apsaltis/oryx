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
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.oryx.common.collection;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A simplified and streamlined version of {@link java.util.BitSet}.
 *
 * @author Sean Owen
 * @author Mahout
 */
public final class BitSet implements Serializable, Cloneable {
  
  private final long[] bits;

  /**
   * Creates a {@code BitSet} that can hold at least the given number of bits.
   *
   * @param numBits number of bits to accommodate.
   */
  public BitSet(int numBits) {
    Preconditions.checkArgument(numBits >= 0);
    int numLongs = numBits >>> 6;
    if ((numBits & 0x3F) != 0) {
      numLongs++;
    }
    bits = new long[numLongs];
  }
  
  private BitSet(long[] bits) {
    this.bits = bits;
  }

  /**
   * @return true iff given bit is set
   */
  public boolean get(int index) {
    // skipping range check for speed
    return (bits[index >>> 6] & 1L << (index & 0x3F)) != 0L;
  }

  /**
   * Sets given bit to "1" or "true".
   */
  public void set(int index) {
    // skipping range check for speed
    bits[index >>> 6] |= 1L << (index & 0x3F);
  }

  /**
   * Sets range bit of bits from {@code from} (inclusive) to {@code to} (exclusive) to "1" or "true".
   */
  public void set(int from, int to) {
    // Not optimized
    for (int i = from; i < to; i++) {
      set(i);
    }
  }

  /**
   * Clears given bit -- sets to "0" or "false".
   */
  public void clear(int index) {
    // skipping range check for speed
    bits[index >>> 6] &= ~(1L << (index & 0x3F));
  }

  /**
   * Clears all bits.
   */
  public void clear() {
    Arrays.fill(bits, 0L);
  }

  /**
   * @return number of bits set
   */
  public int cardinality() {
    int sum = 0;
    for (long l : bits) {
      sum += Long.bitCount(l);
    }
    return sum;
  }

  /**
   * @param index index from which to look for a set bit
   * @return the index of the set bit whose index is lowest and >= the given index, or -1 if no such bit exists
   */
  public int nextSetBit(int index) {
    int offset = index >>> 6;
    int offsetInLong = index & 0x3F;
    long mask = ~((1L << offsetInLong) - 1);
    while (offset < bits.length && (bits[offset] & mask) == 0) {
      offset++;
      mask = -1L;
    }
    if (offset == bits.length) {
      return -1;
    }
    return (offset << 6) + Long.numberOfTrailingZeros(bits[offset] & mask);
  }
  
  @Override
  public BitSet clone() {
    return new BitSet(bits.clone());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bits);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BitSet)) {
      return false;
    }
    BitSet other = (BitSet) o;
    return Arrays.equals(bits, other.bits);
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder(64 * bits.length);
    for (long l : bits) {
      for (int j = 0; j < 64; j++) {
        result.append((l & 1L << j) == 0 ? '0' : '1');
      }
      result.append(' ');
    }
    return result.toString();
  }
  
}
