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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.FastMath;

import com.cloudera.oryx.common.iterator.AbstractLongPrimitiveIterator;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.random.RandomUtils;

/**
 * Based on Mahout's {@code FastIDSet}.
 *
 * @author Sean Owen
 * @author Mahout
 */
public final class LongSet implements Serializable, Cloneable, Iterable<Long> {

  private static final double LOAD_FACTOR = 1.25;
  private static final int MAX_SIZE = (int) (RandomUtils.MAX_INT_SMALLER_TWIN_PRIME / LOAD_FACTOR);

  /** Dummy object used to represent a key that has been removed. */
  private static final long REMOVED = Long.MAX_VALUE;
  private static final long NULL = Long.MIN_VALUE;

  // For faster access
  long[] keys;
  private int numEntries;
  private int numSlotsUsed;
  
  /** Creates a new {@code LongSet} with default capacity. */
  public LongSet() {
    this(2);
  }

  /**
   * Creates a new {@code LongSet} with the given initial capacity.
   *
   * @param initialCapacity initial capacity of set
   */
  public LongSet(int initialCapacity) {
    Preconditions.checkArgument(initialCapacity >= 0, "initialCapacity must be at least 0");
    Preconditions.checkArgument(initialCapacity < MAX_SIZE, "initialCapacity must be less than %d", MAX_SIZE);
    int hashSize = RandomUtils.nextTwinPrime((int) (LOAD_FACTOR * initialCapacity) + 1);
    keys = new long[hashSize];
    Arrays.fill(keys, NULL);
  }
  
  /**
   * @see #findForAdd(long)
   */
  private int find(long key) {
    int theHashCode = (int) key & 0x7FFFFFFF; // make sure it's positive
    long[] keys = this.keys;
    int hashSize = keys.length;
    int jump = 1 + theHashCode % (hashSize - 2);
    int index = theHashCode % hashSize;
    long currentKey = keys[index];
    while (currentKey != NULL && key != currentKey) { // note: true when currentKey == REMOVED
      index -= index < jump ? jump - hashSize : jump;
      currentKey = keys[index];
    }
    return index;
  }
  
  /**
   * @see #find(long)
   */
  private int findForAdd(long key) {
    int theHashCode = (int) key & 0x7FFFFFFF; // make sure it's positive
    long[] keys = this.keys;
    int hashSize = keys.length;
    int jump = 1 + theHashCode % (hashSize - 2);
    int index = theHashCode % hashSize;
    long currentKey = keys[index];
    while (currentKey != NULL && currentKey != REMOVED && key != currentKey) {
      index -= index < jump ? jump - hashSize : jump;
      currentKey = keys[index];
    }
    if (currentKey != REMOVED) {
      return index;
    }
    // If we're adding, it's here, but, the key might have a value already later
    int addIndex = index;
    while (currentKey != NULL && key != currentKey) {
      index -= index < jump ? jump - hashSize : jump;
      currentKey = keys[index];
    }
    return key == currentKey ? index : addIndex;
  }

  /**
   * @return number of items in the set
   */
  public int size() {
    return numEntries;
  }

  /**
   * @return true iff the set is empty
   */
  public boolean isEmpty() {
    return numEntries == 0;
  }

  /**
   * @param key key to check for membership
   * @return true iff the key is present in this set
   */
  public boolean contains(long key) {
    return key != NULL && key != REMOVED && keys[find(key)] != NULL;
  }

  /**
   * @param key key to add to set
   * @return true if the value was not already in the set
   */
  public boolean add(long key) {
    Preconditions.checkArgument(key != NULL && key != REMOVED);
    // If many slots are used, let's clear it up
    if (numSlotsUsed * LOAD_FACTOR >= keys.length) {
      // If over half the slots used are actual entries, let's grow
      if (numEntries * LOAD_FACTOR >= numSlotsUsed) {
        growAndRehash();
      } else {
        // Otherwise just rehash to clear REMOVED entries and don't grow
        rehash();
      }
    }
    // Here we may later consider implementing Brent's variation described on page 532
    int index = findForAdd(key);
    long keyIndex = keys[index];
    if (keyIndex != key) {
      keys[index] = key;
      numEntries++;
      if (keyIndex == NULL) {
        numSlotsUsed++;
      }
      return true;
    }
    return false;
  }
  
  @Override
  public LongPrimitiveIterator iterator() {
    return new KeyIterator();
  }

  /**
   * @return array of all keys in the set, in no particular order
   */
  public long[] toArray() {
    long[] result = new long[numEntries];
    int position = 0;
    for (int i = 0; i < result.length; i++) {
      while (keys[position] == NULL || keys[position] == REMOVED) {
        position++;
      }
      result[i] = keys[position++];
    }
    return result;
  }

  /**
   * @param key key to remove from set
   * @return true if the item existed in the set
   */
  public boolean remove(long key) {
    if (key == NULL || key == REMOVED) {
      return false;
    }
    int index = find(key);
    if (keys[index] == NULL) {
      return false;
    }
    keys[index] = REMOVED;
    numEntries--;
    return true;
  }

  /**
   * @param keys keys to add to this set
   * @return true if any of the values was not previously in the set
   */
  public boolean addAll(long[] keys) {
    boolean changed = false;
    for (long key : keys) {
      if (add(key)) {
        changed = true;
      }
    }
    return changed;
  }

  /**
   * @param keys keys to add to this set
   * @return true if any of the values was not previously in the set
   */
  public boolean addAll(LongSet keys) {
    boolean changed = false;
    for (long key : keys.keys) {
      if (key != NULL && key != REMOVED && add(key)) {
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Removes all keys from the set.
   */
  public void clear() {
    numEntries = 0;
    numSlotsUsed = 0;
    Arrays.fill(keys, NULL);
  }
  
  private void growAndRehash() {
    Preconditions.checkState(keys.length * LOAD_FACTOR < RandomUtils.MAX_INT_SMALLER_TWIN_PRIME,
                             "Can't grow any more");
    rehash(RandomUtils.nextTwinPrime((int) (LOAD_FACTOR * keys.length) + 1));
  }

  /**
   * Optimizes internal storage of keys by removing spaces held by previously removed keys.
   */
  public void rehash() {
    rehash(RandomUtils.nextTwinPrime((int) (LOAD_FACTOR * numEntries) + 1));
  }
  
  private void rehash(int newHashSize) {
    long[] oldKeys = keys;
    numEntries = 0;
    numSlotsUsed = 0;
    keys = new long[newHashSize];
    Arrays.fill(keys, NULL);
    for (long key : oldKeys) {
      if (key != NULL && key != REMOVED) {
        add(key);
      }
    }
  }
  
  /**
   * Convenience method to quickly compute just the size of the intersection with another .
   * 
   * @param other
   *           to intersect with
   * @return number of elements in intersection
   */
  public int intersectionSize(LongSet other) {
    int count = 0;
    for (long key : other.keys) {
      if (key != NULL && key != REMOVED && keys[find(key)] != NULL) {
        count++;
      }
    }
    return count;
  }
  
  @Override
  public LongSet clone() {
    LongSet clone;
    try {
      clone = (LongSet) super.clone();
    } catch (CloneNotSupportedException cnse) {
      throw new AssertionError(cnse);
    }
    clone.keys = keys.clone();
    return clone;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    long[] keys = this.keys;
    for (long key : keys) {
      if (key != NULL && key != REMOVED) {
        hash = 31 * hash + ((int) (key >> 32) ^ (int) key);
      }
    }
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LongSet)) {
      return false;
    }
    LongSet otherMap = (LongSet) other;
    long[] otherKeys = otherMap.keys;
    int length = keys.length;
    int otherLength = otherKeys.length;
    int max = FastMath.min(length, otherLength);

    int i = 0;
    while (i < max) {
      long key = keys[i];
      long otherKey = otherKeys[i];
      if (key == NULL || key == REMOVED) {
        if (otherKey != NULL && otherKey != REMOVED) {
          return false;
        }
      } else {
        if (key != otherKey) {
          return false;
        }
      }
      i++;
    }
    while (i < length) {
      long key = keys[i];
      if (key != NULL && key != REMOVED) {
        return false;
      }
      i++;
    }
    while (i < otherLength) {
      long key = otherKeys[i];
      if (key != NULL && key != REMOVED) {
        return false;
      }
      i++;
    }
    return true;
  }
  
  @Override
  public String toString() {
    if (isEmpty()) {
      return "[]";
    }
    StringBuilder result = new StringBuilder();
    result.append('[');
    for (long key : keys) {
      if (key != NULL && key != REMOVED) {
        result.append(key).append(',');
      }
    }
    result.setCharAt(result.length() - 1, ']');
    return result.toString();
  }
  
  private final class KeyIterator extends AbstractLongPrimitiveIterator {
    
    private int position;
    private int lastNext = -1;
    
    @Override
    public boolean hasNext() {
      goToNext();
      return position < keys.length;
    }
    
    @Override
    public long nextLong() {
      goToNext();
      lastNext = position;
      if (position >= keys.length) {
        throw new NoSuchElementException();
      }
      return keys[position++];
    }

    void goToNext() {
      int length = keys.length;
      while (position < length
             && (keys[position] == NULL || keys[position] == REMOVED)) {
        position++;
      }
    }
    
    @Override
    public void remove() {
      if (lastNext >= keys.length) {
        throw new NoSuchElementException();
      }
      if (lastNext < 0) {
        throw new IllegalStateException();
      }
      keys[lastNext] = REMOVED;
      numEntries--;
    }
    
    public Iterator<Long> iterator() {
      return new KeyIterator();
    }
    
    @Override
    public void skip(int n) {
      position += n;
    }
    
  }
  
}
