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

package com.cloudera.oryx.als.common.lsh;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.random.RandomManager;

/**
 * <p>This class implements a form of location sensitive hashing (LSH). This is used to quickly, approximately,
 * find the vectors in the same direction as a given vector in a vector space. This is useful in, for example, making
 * recommendations, where the best recommendations are the item vectors with largest dot product with
 * the user vector. And, in turn, the largest dot products are found from vectors that point in the same direction
 * from the origin as the user vector -- small angle between them.</p>
 *
 * <p>This uses H hash functions, where the hash function is based on a short vector in a random direction in
 * the space. It suffices to choose a vector whose elements are, randomly, -1 or 1. This is represented as a
 * {@code boolean[]}. The vector defines a hyperplane through the origin, and produces a hash value of 1 or 0
 * depending on whether the given vector is on one side of the hyperplane or the other. This amounts to
 * evaluating whether the dot product of the random vector and given vector is positive or not.</p>
 *
 * <p>These H 1/0 hash values are combined into a signature of H bits, which are represented as an {@code long}
 * because for purposes here, H <= 64.</p>
 *
 * <p>"Close" vectors -- those which form small angles together -- point in nearly the same direction and so
 * should generally fall on the same sides of these hyperplanes. That is, they should match in most bits.</p>
 *
 * <p>As a preprocessing step, all item vector signatures are computed, and these define a sort of
 * hash bucket key for item vectors. Item vectors are put into their buckets.</p>
 *
 * <p>To produce a list of candidate item vectors for a given user vector, the user vector's signature is
 * computed. All buckets whose signature matches in "most" bits are matches, and all item vectors inside
 * are candidates.</p>
 *
 * <p><em>This is experimental, and is disabled unless "model.lsh.sample-ratio" is set to a value less than 1.</em></p>
 *
 * @author Sean Owen
 */
public final class LocationSensitiveHash {

  private static final Logger log = LoggerFactory.getLogger(LocationSensitiveHash.class);

  private final LongObjectMap<float[]> Y;
  private final boolean[][] randomVectors;
  private final double[] meanVector;
  private final LongObjectMap<long[]> buckets;
  private final LongSet newItems;
  private final int maxBitsDiffering;

  /**
   * @param Y item vectors to hash
   */
  public LocationSensitiveHash(LongObjectMap<float[]> Y, double lshSampleRatio, int numHashes) {
    Preconditions.checkNotNull(Y);
    Preconditions.checkArgument(!Y.isEmpty(), "Y is empty");

    Preconditions.checkArgument(lshSampleRatio > 0.0 && lshSampleRatio <= 1.0, "Bad LSH ratio: %s", lshSampleRatio);
    Preconditions.checkArgument(numHashes >= 1 && numHashes <= 64, "Bad # hashes: %s", numHashes);

    this.Y = Y;

    log.info("Using LSH sampling to sample about {}% of items", lshSampleRatio * 100.0);

    // This follows from the binomial distribution:
    double cumulativeProbability = 0.0;
    double denominator = FastMath.pow(2.0, numHashes);
    int bitsDiffering = -1;
    while (bitsDiffering < numHashes && cumulativeProbability < lshSampleRatio) {
      bitsDiffering++;
      cumulativeProbability +=
          ArithmeticUtils.binomialCoefficientDouble(numHashes, bitsDiffering) / denominator;
    }

    maxBitsDiffering = bitsDiffering - 1;
    log.info("Max bits differing: {}", maxBitsDiffering);

    int features = Y.entrySet().iterator().next().getValue().length;

    RandomGenerator random = RandomManager.getRandom();
    randomVectors = new boolean[numHashes][features];
    for (boolean[] randomVector : randomVectors) {
      for (int j = 0; j < features; j++) {
        randomVector[j] = random.nextBoolean();
      }
    }

    meanVector = findMean(Y, features);

    buckets = new LongObjectMap<long[]>();
    int count = 0;
    int maxBucketSize = 0;
    for (LongObjectMap.MapEntry<float[]> entry : Y.entrySet()) {
      long signature = toBitSignature(entry.getValue());
      long[] ids = buckets.get(signature);
      if (ids == null) {
        buckets.put(signature, new long[] {entry.getKey()});
      } else {
        int length = ids.length;
        // Large majority of arrays will be length 1; all are short.
        // This is a reasonable way to store 'sets' of longs
        long[] newIDs = new long[length + 1];
        for (int i = 0; i < length; i++) {
          newIDs[i] = ids[i];
        }
        newIDs[length] = entry.getKey();
        maxBucketSize = FastMath.max(maxBucketSize, newIDs.length);
        buckets.put(signature, newIDs);
      }
      if (++count % 1000000 == 0) {
        log.info("Bucketed {} items", count);
      }
    }

    log.info("Max bucket size {}", maxBucketSize);
    log.info("Put {} items into {} buckets", Y.size(), buckets.size());
    // A separate bucket for new items, which will always be considered
    newItems = new LongSet();
  }

  private static double[] findMean(LongObjectMap<float[]> Y, int features) {
    double[] theMeanVector = new double[features];
    for (LongObjectMap.MapEntry<float[]> entry : Y.entrySet()) {
      float[] vec = entry.getValue();
      for (int i = 0; i < features; i++) {
        theMeanVector[i] += vec[i];
      }
    }
    int size = Y.size();
    for (int i = 0; i < features; i++) {
      theMeanVector[i] /= size;
    }
    return theMeanVector;
  }

  private long toBitSignature(float[] vector) {
    long l = 0L;
    double[] theMeanVector = meanVector;
    for (boolean[] randomVector : randomVectors) {
      // Dot product. true == +1, false == -1
      double total = 0.0;
      for (int i = 0; i < randomVector.length; i++) {
        double delta = vector[i] - theMeanVector[i];
        if (randomVector[i]) {
          total += delta;
        } else {
          total -= delta;
        }
      }
      if (total > 0.0) {
        l = (l << 1L) | 1L;
      } else {
        l <<= 1;
      }
    }
    return l;
  }

  public Collection<Iterator<LongObjectMap.MapEntry<float[]>>> getCandidateIterator(float[][] userVectors) {
    long[] bitSignatures = new long[userVectors.length];
    for (int i = 0; i < userVectors.length; i++) {
      bitSignatures[i] = toBitSignature(userVectors[i]);
    }
    Collection<Iterator<LongObjectMap.MapEntry<float[]>>> inputs = Lists.newArrayList();
    for (LongObjectMap.MapEntry<long[]> entry : buckets.entrySet()) {
      for (long bitSignature : bitSignatures) {
        if (Long.bitCount(bitSignature ^ entry.getKey()) <= maxBitsDiffering) { // # bits differing
          inputs.add(new IDArrayToEntryIterator(entry.getValue()));
          break;
        }
      }
    }

    synchronized (newItems) {
      if (!newItems.isEmpty()) {
        // Have to clone because it's being written to
        inputs.add(new IDToEntryIterator(newItems.clone().iterator()));
      }
    }

    return inputs;
  }

  public void addItem(String itemID) {
    if (newItems != null) {
      long longItemID = StringLongMapping.toLong(itemID);
      synchronized (newItems) {
        newItems.add(longItemID);
      }
    }
  }

  /**
   * @see IDArrayToEntryIterator
   */
  private final class IDToEntryIterator implements Iterator<LongObjectMap.MapEntry<float[]>> {

    private final LongPrimitiveIterator input;
    private final MutableMapEntry delegate;

    private IDToEntryIterator(LongPrimitiveIterator input) {
      this.input = input;
      this.delegate = new MutableMapEntry();
    }

    @Override
    public boolean hasNext() {
      return input.hasNext();
    }

    @Override
    public LongObjectMap.MapEntry<float[]> next() {
      // Will throw NoSuchElementException if needed:
      long itemID = input.nextLong();
      delegate.set(itemID, Y.get(itemID));
      return delegate;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * @see IDToEntryIterator
   */
  private final class IDArrayToEntryIterator implements Iterator<LongObjectMap.MapEntry<float[]>> {

    private int offset;
    private final long[] input;
    private final MutableMapEntry delegate;

    private IDArrayToEntryIterator(long[] input) {
      this.input = input;
      this.delegate = new MutableMapEntry();
    }

    @Override
    public boolean hasNext() {
      return offset < input.length;
    }

    @Override
    public LongObjectMap.MapEntry<float[]> next() {
      if (offset >= input.length) {
        throw new NoSuchElementException();
      }
      long itemID = input[offset++];
      delegate.set(itemID, Y.get(itemID));
      return delegate;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  private static final class MutableMapEntry implements LongObjectMap.MapEntry<float[]> {

    private long key;
    private float[] value;

    @Override
    public long getKey() {
      return key;
    }

    @Override
    public float[] getValue() {
      return value;
    }

    public void set(long key, float[] value) {
      this.key = key;
      this.value = value;
    }
  }

}
