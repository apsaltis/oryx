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

package com.cloudera.oryx.kmeans.common;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.math3.linear.RealVector;

/**
 * Represents a collection of {@code Vector} instances that act as the centers of
 * a set of clusters, as in a k-means model.
 */
public final class Centers implements Iterable<RealVector> {

  // The vectors, where each vector is the center of a particular cluster
  private final List<RealVector> centers;
  
  /**
   * Create a new instance from the given points. Any duplicate
   * points in the arg list will be removed.
   * 
   * @param points The points
   * @throws IllegalArgumentException if no points are given
   */
  public Centers(RealVector... points) {
    this(Arrays.asList(points));
  }
  
  /**
   * Create a new instance from the given points. Any duplicate
   * points in the {@code Iterable} instance will be removed.
   * 
   * @param points The points
   * @throws IllegalArgumentException if the input is empty
   */
  public Centers(Iterable<RealVector> points) {
    this.centers = ImmutableList.copyOf(Sets.newLinkedHashSet(points));
  }
  
  /**
   * Returns the number of points in this instance.
   */
  public int size() {
    return centers.size();
  }
  
  /**
   * Returns the {@code Vector} at the given index.
   */
  public RealVector get(int index) {
    return centers.get(index);
  }

  public boolean contains(RealVector vec) {
    return centers.contains(vec);
  }

  @Override
  public Iterator<RealVector> iterator() {
    return centers.iterator();
  }

  /**
   * Construct a new {@code Centers} object made up of the given {@code Vector}
   * and the points contained in this instance.
   * 
   * @param point The new point
   * @return A new {@code Centers} instance
   */
  public Centers extendWith(RealVector point) {
    return new Centers(Iterables.concat(centers, ImmutableList.of(point)));
  }
  
  /**
   * Returns the minimum squared Euclidean distance between the given
   * {@code Vector} and a point contained in this instance.
   * 
   * @param point The point
   * @return The minimum squared Euclidean distance from the point 
   */
  public Distance getDistance(RealVector point) {
    double min = Double.POSITIVE_INFINITY;
    int index = -1;
    for (int i = 0; i < centers.size(); i++) {
      RealVector c = centers.get(i);
      double distance = c.getDistance(point);
      double distanceSquared = distance * distance;
      if (distanceSquared < min) {
        min = distanceSquared;
        index = i;
      }
      min = Math.min(min, distance * distance);
    }
    return new Distance(min, index);
  }
  
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Centers)) {
      return false;
    }
    Centers c = (Centers) other;
    return centers.containsAll(c.centers) && c.centers.containsAll(centers);
  }
  
  @Override
  public int hashCode() {
    int hc = 0;
    for (RealVector center : centers) {
      hc += center.hashCode();
    }
    return hc;
  }
  
  @Override
  public String toString() {
    return centers.toString();
  }
}
