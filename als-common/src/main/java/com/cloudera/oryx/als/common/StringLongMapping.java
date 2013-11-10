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

package com.cloudera.oryx.als.common;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.random.RandomUtils;

/**
 * Encapsulates a mapping from {@link String} to {@code long} and back. Given {@link String}s, it hashes
 * them to {@code long} and stores the mapping, so that the original {@link String} can be recovered from the
 * {@code long}. In the rare case that two hash to the same value, the more recent mapping "wins" and overwrites
 * a previous value.
 *
 * @author Sean Owen
 */
public final class StringLongMapping {

  // long min/max values are 19 digits. We're looking at only up to 18 digits here.
  // Very big longs will be treated as strings and hashed. This is useful because
  // Long.MIN_VALUE and Long.MAX_VALUE are special values in this system, and yet
  // may appear as an ID in data, not infrequently, just becaue they are these special
  // values. They'll be hashed to something else. Doing so, we can still use the
  // numeric value of numeric strings directly in other cases, and retain that intuitive
  // mapping. (We also save catching an exception in long parsing this way since any
  // long matching this will definitely be parseable)
  private static final Pattern MOST_LONGS_PATTERN = Pattern.compile("^-?\\d{1,18}$");

  private final LongObjectMap<String> reverseMapping;
  private final ReadWriteLock lock;

  public StringLongMapping() {
    reverseMapping = new LongObjectMap<String>();
    lock = new ReentrantReadWriteLock();
  }

  public static long toLong(String id) {
    return MOST_LONGS_PATTERN.matcher(id).matches() ? Long.parseLong(id) : RandomUtils.hash(id);
  }

  /**
   * @param id ID to hash
   * @return hash of {@code id} argument, now stored in the mapping
   */
  public long add(String id) {
    if (MOST_LONGS_PATTERN.matcher(id).matches()) {
      return Long.parseLong(id);
    }
    long numericID = RandomUtils.hash(id);
    addMapping(id, numericID);
    return numericID;
  }

  /**
   * @param id ID to hash
   * @param numericID explicit, supplied hash of {@code id} argument, given to be
   *  stored in the mapping
   */
  public void addMapping(String id, long numericID) {
    Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      reverseMapping.put(numericID, id);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * @param numericID hash value to map back to a {@link String}
   * @return the {@link String} ID that hashed to the value, or if none exists, then simply
   *   the argument as a {@link String}
   */
  public String toString(long numericID) {
    String id = null;
    Lock readLock = lock.readLock();
    readLock.lock();
    try {
      id = reverseMapping.get(numericID);
    } finally {
      readLock.unlock();
    }
    return id == null ? Long.toString(numericID) : id;
  }

  // Careful, this is controlled by the lock

  public LongObjectMap<String> getReverseMapping() {
    return reverseMapping;
  }

  public ReadWriteLock getLock() {
    return lock;
  }

}
