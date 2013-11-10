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
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.FastMath;

import com.cloudera.oryx.common.LangUtils;
import com.cloudera.oryx.common.iterator.AbstractLongPrimitiveIterator;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.random.RandomUtils;

/**
 * Based on Mahout's {@code FastByIDMap}; used with {@code float} instead of {@code double}.
 *
 * This class is not thread-safe.
 *
 * @author Sean Owen
 * @author Mahout
 */
public final class LongFloatMap implements Serializable, Cloneable {

  private static final double LOAD_FACTOR = 1.25;
  private static final int MAX_SIZE = (int) (RandomUtils.MAX_INT_SMALLER_TWIN_PRIME / LOAD_FACTOR);

  /** Dummy object used to represent a key that has been removed. */
  private static final long REMOVED = Long.MAX_VALUE;
  private static final long KEY_NULL = Long.MIN_VALUE;
  private static final float VALUE_NULL = Float.NaN;

  // For faster access:
  long[] keys;
  float[] values;
  private int numEntries;
  private int numSlotsUsed;

  /** Creates a new {@code LongFloatMap} with default capacity. */
  public LongFloatMap() {
    this(2);
  }

  /**
   * Creates a new {@code LongFloatMap} with given initial capacity.
   *
   * @param initialCapacity initial capacity
   */
  public LongFloatMap(int initialCapacity) {
    Preconditions.checkArgument(initialCapacity >= 0, "initialCapacity must be at least 0");
    Preconditions.checkArgument(initialCapacity < MAX_SIZE, "initialCapacity must be less than " + MAX_SIZE);
    int hashSize = RandomUtils.nextTwinPrime((int) (LOAD_FACTOR * initialCapacity) + 1);
    keys = new long[hashSize];
    Arrays.fill(keys, KEY_NULL);
    values = new float[hashSize];
    Arrays.fill(values, VALUE_NULL);
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
    while (currentKey != KEY_NULL && key != currentKey) {
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
    while (currentKey != KEY_NULL && currentKey != REMOVED && key != currentKey) {
      index -= index < jump ? jump - hashSize : jump;
      currentKey = keys[index];
    }
    if (currentKey != REMOVED) {
      return index;
    }
    // If we're adding, it's here, but, the key might have a value already later
    int addIndex = index;
    while (currentKey != KEY_NULL && key != currentKey) {
      index -= index < jump ? jump - hashSize : jump;
      currentKey = keys[index];
    }
    return key == currentKey ? index : addIndex;
  }

  /**
   * @param key key to get value for
   * @return value associated with key or {@link Float#NaN} if there is no such value
   */
  public float get(long key) {
    if (key == KEY_NULL) {
      return VALUE_NULL;
    }
    int index = find(key);
    return values[index];
  }

  // Added:

  /**
   * Adds to the value for a given key. If no mapping exists for the key then the value is set as a new
   * value for the key.
   *
   * @param key key whose value should be incremented
   * @param delta amount to increment value by
   */
  public void increment(long key, float delta) {
    Preconditions.checkArgument(key != KEY_NULL && key != REMOVED);
    int index = find(key);
    float currentValue = values[index];
    if (Float.isNaN(currentValue)) {
      put(key, delta);
    } else {
      values[index] = currentValue + delta;
    }
  }

  /**
   * @return number of mappings set in this map
   */
  public int size() {
    return numEntries;
  }

  /**
   * @return true iff there are no mappings
   */
  public boolean isEmpty() {
    return numEntries == 0;
  }

  /**
   * @param key key to look for
   * @return true if there is a mapping for the key in this map
   */
  public boolean containsKey(long key) {
    return key != KEY_NULL && key != REMOVED && keys[find(key)] != KEY_NULL;
  }

  /**
   * @param key key to map
   * @param value value that the key maps to
   */
  public void put(long key, float value) {
    Preconditions.checkArgument(key != KEY_NULL && key != REMOVED);
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
    if (keyIndex == key) {
      values[index] = value;
    } else {
      keys[index] = key;
      values[index] = value;
      numEntries++;
      if (keyIndex == KEY_NULL) {
        numSlotsUsed++;
      }
    }
  }

  /**
   * @param key key for which mapping should be removed
   */
  public void remove(long key) {
    if (key == KEY_NULL || key == REMOVED) {
      return;
    }
    int index = find(key);
    if (keys[index] != KEY_NULL) {
      keys[index] = REMOVED;
      numEntries--;
      values[index] = VALUE_NULL;
    }
  }

  /**
   * Removes all mappings.
   */
  public void clear() {
    numEntries = 0;
    numSlotsUsed = 0;
    Arrays.fill(keys, KEY_NULL);
    Arrays.fill(values, VALUE_NULL);
  }

  /**
   * @return iterator over keys in the map
   */
  public LongPrimitiveIterator keySetIterator() {
    return new KeyIterator();
  }

  /**
   * @return {@link Set} of entries/mappings in the map as {@link MapEntry}
   */
  public Set<MapEntry> entrySet() {
    return new EntrySet();
  }

  /**
   * Optimizes internal storage of keys by removing spaces held by previously removed keys.
   */
  public void rehash() {
    rehash(RandomUtils.nextTwinPrime((int) (LOAD_FACTOR * numEntries) + 1));
  }

  private void growAndRehash() {
    Preconditions.checkState(keys.length * LOAD_FACTOR < RandomUtils.MAX_INT_SMALLER_TWIN_PRIME,
                             "Can't grow any more");
    rehash(RandomUtils.nextTwinPrime((int) (LOAD_FACTOR * keys.length) + 1));
  }

  private void rehash(int newHashSize) {
    long[] oldKeys = keys;
    float[] oldValues = values;
    numEntries = 0;
    numSlotsUsed = 0;
    keys = new long[newHashSize];
    Arrays.fill(keys, KEY_NULL);
    values = new float[newHashSize];
    Arrays.fill(values, VALUE_NULL);
    int length = oldKeys.length;
    for (int i = 0; i < length; i++) {
      long key = oldKeys[i];
      if (key != KEY_NULL && key != REMOVED) {
        put(key, oldValues[i]);
      }
    }
  }

  void iteratorRemove(int lastNext) {
    if (lastNext >= values.length) {
      throw new NoSuchElementException();
    }
    Preconditions.checkState(lastNext >= 0);
    values[lastNext] = VALUE_NULL;
    keys[lastNext] = REMOVED;
    numEntries--;
  }

  @Override
  public LongFloatMap clone() {
    LongFloatMap clone;
    try {
      clone = (LongFloatMap) super.clone();
    } catch (CloneNotSupportedException cnse) {
      throw new AssertionError(cnse);
    }
    clone.keys = keys.clone();
    clone.values = values.clone();
    return clone;
  }

  @Override
  public String toString() {
    if (isEmpty()) {
      return "{}";
    }
    StringBuilder result = new StringBuilder();
    result.append('{');
    for (int i = 0; i < keys.length; i++) {
      long key = keys[i];
      if (key != KEY_NULL && key != REMOVED) {
        result.append(key).append('=').append(values[i]).append(',');
      }
    }
    result.setCharAt(result.length() - 1, '}');
    return result.toString();
  }

  @Override
  public int hashCode() {
    int hash = 0;
    long[] keys = this.keys;
    int max = keys.length;
    for (int i = 0; i < max; i++) {
      long key = keys[i];
      if (key != KEY_NULL && key != REMOVED) {
        hash = 31 * hash + ((int) (key >> 32) ^ (int) key);
        hash = 31 * hash + LangUtils.hashDouble(values[i]);
      }
    }
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LongFloatMap)) {
      return false;
    }
    LongFloatMap otherMap = (LongFloatMap) other;
    long[] otherKeys = otherMap.keys;
    float[] otherValues = otherMap.values;
    int length = keys.length;
    int otherLength = otherKeys.length;
    int max = FastMath.min(length, otherLength);

    int i = 0;
    while (i < max) {
      long key = keys[i];
      long otherKey = otherKeys[i];
      if (key == KEY_NULL || key == REMOVED) {
        if (otherKey != KEY_NULL && otherKey != REMOVED) {
          return false;
        }
      } else {
        if (key != otherKey || values[i] != otherValues[i]) {
          return false;
        }
      }
      i++;
    }
    while (i < length) {
      long key = keys[i];
      if (key != KEY_NULL && key != REMOVED) {
        return false;
      }
      i++;
    }
    while (i < otherLength) {
      long key = otherKeys[i];
      if (key != KEY_NULL && key != REMOVED) {
        return false;
      }
      i++;
    }
    return true;
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
      int length = values.length;
      while (position < length && Float.isNaN(values[position])) {
        position++;
      }
    }

    @Override
    public void remove() {
      iteratorRemove(lastNext);
    }

    @Override
    public void skip(int n) {
      position += n;
    }

  }

  /**
   * Represents one entry, or mapping, in a {@link LongFloatMap}.
   */
  public interface MapEntry {
    /**
     * @return key in this mapping
     */
    long getKey();
    /**
     * @return value in this mapping
     */
    float getValue();
  }

  private final class MapEntryImpl implements MapEntry {

    private int index;

    void setIndex(int index) {
      this.index = index;
    }

    @Override
    public long getKey() {
      return keys[index];
    }

    @Override
    public float getValue() {
      return values[index];
    }

    @Override
    public String toString() {
      return getKey() + "=" + getValue();
    }

  }

  private final class EntrySet extends AbstractSet<MapEntry> {

    @Override
    public int size() {
      return LongFloatMap.this.size();
    }

    @Override
    public boolean isEmpty() {
      return LongFloatMap.this.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      return containsKey((Long) o);
    }

    @Override
    public Iterator<MapEntry> iterator() {
      return new EntryIterator();
    }

    @Override
    public boolean add(MapEntry t) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends MapEntry> ts) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> objects) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> objects) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      LongFloatMap.this.clear();
    }

  }

  private final class EntryIterator implements Iterator<MapEntry> {

    private int position;
    private int lastNext = -1;
    private final MapEntryImpl entry = new MapEntryImpl();

    @Override
    public boolean hasNext() {
      goToNext();
      return position < keys.length;
    }

    @Override
    public MapEntry next() {
      goToNext();
      lastNext = position;
      if (position >= keys.length) {
        throw new NoSuchElementException();
      }
      entry.setIndex(position++);
      return entry;
    }

    void goToNext() {
      int length = values.length;
      while (position < length && Float.isNaN(values[position])) {
        position++;
      }
    }

    @Override
    public void remove() {
      iteratorRemove(lastNext);
    }
  }

}
