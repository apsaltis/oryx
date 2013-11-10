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

package com.cloudera.oryx.common.math;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.exception.MathArithmeticException;
import org.apache.commons.math3.exception.NotPositiveException;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap;

import com.cloudera.oryx.common.iterator.LongPrimitiveArrayIterator;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;

/**
 * Rescued from Commons Math 3.2.
 */
public final class OpenMapRealVector extends SparseRealVector implements Serializable {

  /** Default Tolerance for having a value considered zero. */
  public static final double DEFAULT_ZERO_TOLERANCE = 1.0e-12;
  /** Serializable version identifier. */
  private static final long serialVersionUID = 8772222695580707260L;
  /** Entries of the vector. */
  private final OpenIntToDoubleHashMap entries;
  /** Dimension of the vector. */
  private final int virtualSize;
  /** Tolerance for having a value considered zero. */
  private final double epsilon;

  /**
   * Build a 0-length vector.
   * Zero-length vectors may be used to initialized construction of vectors
   * by data gathering. We start with zero-length and use either the {@link
   * #OpenMapRealVector(OpenMapRealVector, int)} constructor
   * or one of the {@code append} method ({@link #append(double)},
   * {@link #append(RealVector)}) to gather data into this vector.
   */
  public OpenMapRealVector() {
    this(0, DEFAULT_ZERO_TOLERANCE);
  }

  /**
   * Construct a vector of zeroes.
   *
   * @param dimension Size of the vector.
   */
  public OpenMapRealVector(int dimension) {
    this(dimension, DEFAULT_ZERO_TOLERANCE);
  }

  /**
   * Construct a vector of zeroes, specifying zero tolerance.
   *
   * @param dimension Size of the vector.
   * @param epsilon Tolerance below which a value considered zero.
   */
  public OpenMapRealVector(int dimension, double epsilon) {
    virtualSize = dimension;
    entries = new OpenIntToDoubleHashMap(0.0);
    this.epsilon = epsilon;
  }

  /**
   * Build a resized vector, for use with append.
   *
   * @param v Original vector.
   * @param resize Amount to add.
   */
  OpenMapRealVector(OpenMapRealVector v, int resize) {
    virtualSize = v.getDimension() + resize;
    entries = new OpenIntToDoubleHashMap(v.entries);
    epsilon = v.epsilon;
  }

  /**
   * Build a vector with known the sparseness (for advanced use only).
   *
   * @param dimension Size of the vector.
   * @param expectedSize The expected number of non-zero entries.
   */
  public OpenMapRealVector(int dimension, int expectedSize) {
    this(dimension, expectedSize, DEFAULT_ZERO_TOLERANCE);
  }

  /**
   * Build a vector with known the sparseness and zero tolerance
   * setting (for advanced use only).
   *
   * @param dimension Size of the vector.
   * @param expectedSize Expected number of non-zero entries.
   * @param epsilon Tolerance below which a value is considered zero.
   */
  public OpenMapRealVector(int dimension, int expectedSize, double epsilon) {
    virtualSize = dimension;
    entries = new OpenIntToDoubleHashMap(expectedSize, 0.0);
    this.epsilon = epsilon;
  }

  /**
   * Create from an array.
   * Only non-zero entries will be stored.
   *
   * @param values Set of values to create from.
   */
  public OpenMapRealVector(double[] values) {
    this(values, DEFAULT_ZERO_TOLERANCE);
  }

  /**
   * Create from an array, specifying zero tolerance.
   * Only non-zero entries will be stored.
   *
   * @param values Set of values to create from.
   * @param epsilon Tolerance below which a value is considered zero.
   */
  public OpenMapRealVector(double[] values, double epsilon) {
    virtualSize = values.length;
    entries = new OpenIntToDoubleHashMap(0.0);
    this.epsilon = epsilon;
    for (int key = 0; key < values.length; key++) {
      double value = values[key];
      if (!isDefaultValue(value)) {
        entries.put(key, value);
      }
    }
  }

  /**
   * Create from an array.
   * Only non-zero entries will be stored.
   *
   * @param values The set of values to create from
   */
  public OpenMapRealVector(Double[] values) {
    this(values, DEFAULT_ZERO_TOLERANCE);
  }

  /**
   * Create from an array.
   * Only non-zero entries will be stored.
   *
   * @param values Set of values to create from.
   * @param epsilon Tolerance below which a value is considered zero.
   */
  public OpenMapRealVector(Double[] values, double epsilon) {
    virtualSize = values.length;
    entries = new OpenIntToDoubleHashMap(0.0);
    this.epsilon = epsilon;
    for (int key = 0; key < values.length; key++) {
      double value = values[key];
      if (!isDefaultValue(value)) {
        entries.put(key, value);
      }
    }
  }

  /**
   * Copy constructor.
   *
   * @param v Instance to copy from.
   */
  public OpenMapRealVector(OpenMapRealVector v) {
    virtualSize = v.getDimension();
    entries = new OpenIntToDoubleHashMap(v.getEntries());
    epsilon = v.epsilon;
  }

  /**
   * Generic copy constructor.
   *
   * @param v Instance to copy from.
   */
  public OpenMapRealVector(RealVector v) {
    virtualSize = v.getDimension();
    entries = new OpenIntToDoubleHashMap(0.0);
    epsilon = DEFAULT_ZERO_TOLERANCE;
    for (int key = 0; key < virtualSize; key++) {
      double value = v.getEntry(key);
      if (!isDefaultValue(value)) {
        entries.put(key, value);
      }
    }
  }

  /**
   * Get the entries of this instance.
   *
   * @return the entries of this instance.
   */
  private OpenIntToDoubleHashMap getEntries() {
    return entries;
  }

  /**
   * Determine if this value is within epsilon of zero.
   *
   * @param value Value to test
   * @return {@code true} if this value is within epsilon to zero,
   * {@code false} otherwise.
   */
  boolean isDefaultValue(double value) {
    return FastMath.abs(value) < epsilon;
  }

  @Override
  public RealVector add(RealVector v) {
    checkVectorDimensions(v.getDimension());
    if (v instanceof OpenMapRealVector) {
      return add((OpenMapRealVector) v);
    } else {
      return super.add(v);
    }
  }

  /**
   * Optimized method to add two OpenMapRealVectors.
   * It copies the larger vector, then iterates over the smaller.
   *
   * @param v Vector to add.
   * @return the sum of {@code this} and {@code v}.
   * @throws DimensionMismatchException if the dimensions do not match.
   */
  public OpenMapRealVector add(OpenMapRealVector v) {
    checkVectorDimensions(v.getDimension());
    boolean copyThis = entries.size() > v.entries.size();
    OpenMapRealVector res = copyThis ? this.copy() : v.copy();
    OpenIntToDoubleHashMap.Iterator iter = copyThis ? v.entries.iterator() : entries.iterator();
    OpenIntToDoubleHashMap randomAccess = copyThis ? entries : v.entries;
    while (iter.hasNext()) {
      iter.advance();
      int key = iter.key();
      if (randomAccess.containsKey(key)) {
        res.setEntry(key, randomAccess.get(key) + iter.value());
      } else {
        res.setEntry(key, iter.value());
      }
    }
    return res;
  }

  /**
   * Optimized method to append a OpenMapRealVector.
   * @param v vector to append
   * @return The result of appending {@code v} to self
   */
  public OpenMapRealVector append(OpenMapRealVector v) {
    OpenMapRealVector res = new OpenMapRealVector(this, v.getDimension());
    OpenIntToDoubleHashMap.Iterator iter = v.entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      res.setEntry(iter.key() + virtualSize, iter.value());
    }
    return res;
  }

  @Override
  public OpenMapRealVector append(RealVector v) {
    if (v instanceof OpenMapRealVector) {
      return append((OpenMapRealVector) v);
    }
    OpenMapRealVector res = new OpenMapRealVector(this, v.getDimension());
    for (int i = 0; i < v.getDimension(); i++) {
      res.setEntry(i + virtualSize, v.getEntry(i));
    }
    return res;
  }

  @Override
  public OpenMapRealVector append(double d) {
    OpenMapRealVector res = new OpenMapRealVector(this, 1);
    res.setEntry(virtualSize, d);
    return res;
  }

  @Override
  public OpenMapRealVector copy() {
    return new OpenMapRealVector(this);
  }

  @SuppressWarnings("deprecation")
  @Deprecated
  @Override
  public OpenMapRealVector ebeDivide(RealVector v) {
    checkVectorDimensions(v.getDimension());
    OpenMapRealVector res = new OpenMapRealVector(this);
    /*
     * MATH-803: it is not sufficient to loop through non zero entries of
     * this only. Indeed, if this[i] = 0d and v[i] = 0d, then
     * this[i] / v[i] = NaN, and not 0d.
     */
    int n = getDimension();
    for (int i = 0; i < n; i++) {
      res.setEntry(i, this.getEntry(i) / v.getEntry(i));
    }
    return res;
  }

  @SuppressWarnings("deprecation")
  @Deprecated
  @Override
  public OpenMapRealVector ebeMultiply(RealVector v) {
    checkVectorDimensions(v.getDimension());
    OpenMapRealVector res = new OpenMapRealVector(this);
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      res.setEntry(iter.key(), iter.value() * v.getEntry(iter.key()));
    }
        /*
         * MATH-803: the above loop assumes that 0d * x  = 0d for any double x,
         * which allows to consider only the non-zero entries of this. However,
         * this fails if this[i] == 0d and (v[i] = NaN or v[i] = Infinity).
         *
         * These special cases are handled below.
         */
    if (v.isNaN() || v.isInfinite()) {
      int n = getDimension();
      for (int i = 0; i < n; i++) {
        double y = v.getEntry(i);
        if (Double.isNaN(y)) {
          res.setEntry(i, Double.NaN);
        } else if (Double.isInfinite(y)) {
          double x = this.getEntry(i);
          res.setEntry(i, x * y);
        }
      }
    }
    return res;
  }

  @Override
  public OpenMapRealVector getSubVector(int index, int n) {
    checkIndex(index);
    if (n < 0) {
      throw new NotPositiveException(LocalizedFormats.NUMBER_OF_ELEMENTS_SHOULD_BE_POSITIVE, n);
    }
    checkIndex(index + n - 1);
    OpenMapRealVector res = new OpenMapRealVector(n);
    int end = index + n;
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      int key = iter.key();
      if (key >= index && key < end) {
        res.setEntry(key - index, iter.value());
      }
    }
    return res;
  }

  @Override
  public int getDimension() {
    return virtualSize;
  }

  /**
   * Optimized method to compute distance.
   *
   * @param v Vector to compute distance to.
   * @return the distance from {@code this} and {@code v}.
   * @throws DimensionMismatchException if the dimensions do not match.
   */
  public double getDistance(OpenMapRealVector v) {
    checkVectorDimensions(v.getDimension());
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    double res = 0;
    while (iter.hasNext()) {
      iter.advance();
      int key = iter.key();
      double delta = iter.value() - v.getEntry(key);
      res += delta * delta;
    }
    iter = v.getEntries().iterator();
    while (iter.hasNext()) {
      iter.advance();
      int key = iter.key();
      if (!entries.containsKey(key)) {
        double value = iter.value();
        res += value * value;
      }
    }
    return FastMath.sqrt(res);
  }

  @Override
  public double getDistance(RealVector v) {
    checkVectorDimensions(v.getDimension());
    if (v instanceof OpenMapRealVector) {
      return getDistance((OpenMapRealVector) v);
    } else {
      return super.getDistance(v);
    }
  }

  @Override
  public double getEntry(int index) {
    checkIndex(index);
    return entries.get(index);
  }

  /**
   * Distance between two vectors.
   * This method computes the distance consistent with
   * L<sub>1</sub> norm, i.e. the sum of the absolute values of
   * elements differences.
   *
   * @param v Vector to which distance is requested.
   * @return distance between this vector and {@code v}.
   * @throws DimensionMismatchException if the dimensions do not match.
   */
  public double getL1Distance(OpenMapRealVector v) {
    checkVectorDimensions(v.getDimension());
    double max = 0;
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      double delta = FastMath.abs(iter.value() - v.getEntry(iter.key()));
      max += delta;
    }
    iter = v.getEntries().iterator();
    while (iter.hasNext()) {
      iter.advance();
      int key = iter.key();
      if (!entries.containsKey(key)) {
        double delta = FastMath.abs(iter.value());
        max +=  FastMath.abs(delta);
      }
    }
    return max;
  }

  @Override
  public double getL1Distance(RealVector v) {
    checkVectorDimensions(v.getDimension());
    if (v instanceof OpenMapRealVector) {
      return getL1Distance((OpenMapRealVector) v);
    } else {
      return super.getL1Distance(v);
    }
  }

  /**
   * Optimized method to compute LInfDistance.
   *
   * @param v Vector to compute distance from.
   * @return the LInfDistance.
   * @throws DimensionMismatchException if the dimensions do not match.
   */
  private double getLInfDistance(OpenMapRealVector v) {
    checkVectorDimensions(v.getDimension());
    double max = 0;
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      double delta = FastMath.abs(iter.value() - v.getEntry(iter.key()));
      if (delta > max) {
        max = delta;
      }
    }
    iter = v.getEntries().iterator();
    while (iter.hasNext()) {
      iter.advance();
      int key = iter.key();
      if (!entries.containsKey(key) && iter.value() > max) {
        max = iter.value();
      }
    }
    return max;
  }

  @Override
  public double getLInfDistance(RealVector v) {
    checkVectorDimensions(v.getDimension());
    if (v instanceof OpenMapRealVector) {
      return getLInfDistance((OpenMapRealVector) v);
    } else {
      return super.getLInfDistance(v);
    }
  }

  @Override
  public boolean isInfinite() {
    boolean infiniteFound = false;
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      double value = iter.value();
      if (Double.isNaN(value)) {
        return false;
      }
      if (Double.isInfinite(value)) {
        infiniteFound = true;
      }
    }
    return infiniteFound;
  }

  @Override
  public boolean isNaN() {
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      if (Double.isNaN(iter.value())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public OpenMapRealVector mapAdd(double d) {
    return copy().mapAddToSelf(d);
  }

  @Override
  public OpenMapRealVector mapAddToSelf(double d) {
    for (int i = 0; i < virtualSize; i++) {
      setEntry(i, getEntry(i) + d);
    }
    return this;
  }

  @Override
  public void setEntry(int index, double value) {
    checkIndex(index);
    if (!isDefaultValue(value)) {
      entries.put(index, value);
    } else if (entries.containsKey(index)) {
      entries.remove(index);
    }
  }

  @Override
  public void setSubVector(int index, RealVector v) {
    checkIndex(index);
    checkIndex(index + v.getDimension() - 1);
    for (int i = 0; i < v.getDimension(); i++) {
      setEntry(i + index, v.getEntry(i));
    }
  }

  @Override
  public void set(double value) {
    for (int i = 0; i < virtualSize; i++) {
      setEntry(i, value);
    }
  }

  /**
   * Optimized method to subtract OpenMapRealVectors.
   *
   * @param v Vector to subtract from {@code this}.
   * @return the difference of {@code this} and {@code v}.
   * @throws DimensionMismatchException if the dimensions do not match.
   */
  public OpenMapRealVector subtract(OpenMapRealVector v) {
    checkVectorDimensions(v.getDimension());
    OpenMapRealVector res = copy();
    OpenIntToDoubleHashMap.Iterator iter = v.getEntries().iterator();
    while (iter.hasNext()) {
      iter.advance();
      int key = iter.key();
      if (entries.containsKey(key)) {
        res.setEntry(key, entries.get(key) - iter.value());
      } else {
        res.setEntry(key, -iter.value());
      }
    }
    return res;
  }

  @Override
  public RealVector subtract(RealVector v) {
    checkVectorDimensions(v.getDimension());
    if (v instanceof OpenMapRealVector) {
      return subtract((OpenMapRealVector) v);
    } else {
      return super.subtract(v);
    }
  }

  @Override
  public OpenMapRealVector unitVector() {
    OpenMapRealVector res = copy();
    res.unitize();
    return res;
  }

  @Override
  public void unitize() {
    double norm = getNorm();
    if (isDefaultValue(norm)) {
      throw new MathArithmeticException(LocalizedFormats.ZERO_NORM);
    }
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      entries.put(iter.key(), iter.value() / norm);
    }
  }

  @Override
  public double[] toArray() {
    double[] res = new double[virtualSize];
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      res[iter.key()] = iter.value();
    }
    return res;
  }

  /**
   * {@inheritDoc}
   * Implementation Note: This works on exact values, and as a result
   * it is possible for {@code a.subtract(b)} to be the zero vector, while
   * {@code a.hashCode() != b.hashCode()}.
   */
  @Override
  public int hashCode() {
    int prime = 31;
    int result = 1;
    long temp = Double.doubleToLongBits(epsilon);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + virtualSize;
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      temp = Double.doubleToLongBits(iter.value());
      result = prime * result + (int) (temp ^ (temp >>32));
    }
    return result;
  }

  /**
   * {@inheritDoc}
   * Implementation Note: This performs an exact comparison, and as a result
   * it is possible for {@code a.subtract(b}} to be the zero vector, while
   * {@code  a.equals(b) == false}.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OpenMapRealVector)) {
      return false;
    }
    OpenMapRealVector other = (OpenMapRealVector) obj;
    if (virtualSize != other.virtualSize) {
      return false;
    }
    if (Double.doubleToLongBits(epsilon) !=
        Double.doubleToLongBits(other.epsilon)) {
      return false;
    }
    OpenIntToDoubleHashMap.Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      iter.advance();
      double test = other.getEntry(iter.key());
      if (Double.doubleToLongBits(test) != Double.doubleToLongBits(iter.value())) {
        return false;
      }
    }
    iter = other.getEntries().iterator();
    while (iter.hasNext()) {
      iter.advance();
      double test = iter.value();
      if (Double.doubleToLongBits(test) != Double.doubleToLongBits(getEntry(iter.key()))) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return the percentage of none zero elements as a decimal percent.
   */
  public double getSparsity() {
    return (double)entries.size()/(double)getDimension();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Iterator<Entry> sparseIterator() {
    return new OpenMapSparseIterator();
  }

  @Override
  public Iterator<Entry> sparseInOrderIterator() {
    return new OpenMapSparseInOrderIterator();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterator<Entry> inOrderIter = sparseInOrderIterator();
    while (inOrderIter.hasNext()) {
      Entry e = inOrderIter.next();
      sb.append(e.getIndex()).append(':').append(e.getValue()).append(' ');
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  /**
   * Implementation of {@code Entry} optimized for OpenMap.
   * This implementation does not allow arbitrary calls to {@code setIndex}
   * since the order in which entries are returned is undefined.
   */
  protected final class OpenMapEntry extends Entry {
    /** Iterator pointing to the entry. */
    private final OpenIntToDoubleHashMap.Iterator iter;

    /**
     * Build an entry from an iterator point to an element.
     *
     * @param iter Iterator pointing to the entry.
     */
    OpenMapEntry(OpenIntToDoubleHashMap.Iterator iter) {
      this.iter = iter;
    }

    @Override
    public double getValue() {
      return iter.value();
    }

    @Override
    public void setValue(double value) {
      entries.put(iter.key(), value);
    }

    @Override
    public int getIndex() {
      return iter.key();
    }

  }

  /**
   * Iterator class to do iteration over just the non-zero elements.
   * This implementation is fail-fast, so cannot be used to modify
   * any zero element.
   */
  protected final class OpenMapSparseIterator implements Iterator<Entry> {
    /** Underlying iterator. */
    private final OpenIntToDoubleHashMap.Iterator iter;
    /** Current entry. */
    private final Entry current;

    /** Simple constructor. */
    OpenMapSparseIterator() {
      iter = entries.iterator();
      current = new OpenMapEntry(iter);
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Entry next() {
      iter.advance();
      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported");
    }
  }

  // Added for Oryx:

  @Override
  public int getNumEntries() {
    return entries.size();
  }

  protected final class OpenMapSparseInOrderIterator implements Iterator<Entry> {

    private final LongPrimitiveIterator iter;
    private final Entry current;

    OpenMapSparseInOrderIterator() {
      List<Integer> indices = Lists.newArrayListWithCapacity(entries.size());
      OpenIntToDoubleHashMap.Iterator it = entries.iterator();
      while (it.hasNext()) {
        it.advance();
        indices.add(it.key());
      }
      long[] indicesArray = new long[indices.size()];
      for (int i = 0; i < indicesArray.length; i++) {
        indicesArray[i] = indices.get(i);
      }
      Arrays.sort(indicesArray);
      this.iter = new LongPrimitiveArrayIterator(indicesArray);
      current = new Entry();
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Entry next() {
      current.setIndex(iter.next().intValue());
      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported");
    }
  }

}
