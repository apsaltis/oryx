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

package com.cloudera.oryx.kmeans.computation.cluster;

import com.cloudera.oryx.common.math.AbstractRealVectorPreservingVisitor;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.kmeans.common.Centers;
import com.cloudera.oryx.kmeans.common.Distance;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.evaluate.ClosestSketchVectorData;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.RandomGenerator;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.SortedSet;

/**
 * An internal data structure that manages the locations of the current centers during
 * k-means|| processing.
 */
public final class KSketchIndex implements Serializable {

  private final int[] pointsPerFold;
  private final List<List<BitSet>> indices;
  private final List<List<RealVector>> points;
  private final List<List<Double>> lengthSquared;
  private final int dimensions;
  private final int projectionBits;
  private final int projectionSamples;
  private final long seed;
  private double[] projection;
  private boolean updated;
  
  public KSketchIndex(int numFolds, int dimensions, int projectionBits, int projectionSamples, long seed) {
    this.pointsPerFold = new int[numFolds];
    this.indices = Lists.newArrayList();
    this.points = Lists.newArrayList();
    this.lengthSquared = Lists.newArrayList();
    for (int i = 0; i < numFolds; i++) {
      points.add(Lists.<RealVector>newArrayList());
      lengthSquared.add(Lists.<Double>newArrayList());
    }
    this.dimensions = dimensions;
    this.projectionBits = projectionBits;
    this.projectionSamples = projectionSamples;
    this.seed = seed;
  }
  
  public KSketchIndex(List<Centers> centers, int projectionBits, int projectionSamples, long seed) {
    this(centers.size(), centers.get(0).get(0).getDimension(), projectionBits, projectionSamples, seed);
    for (int centerId = 0; centerId < centers.size(); centerId++) {
      for (RealVector v : centers.get(centerId)) {
        add(v, centerId);
      }
    }
  }

  public int getDimension() {
    return dimensions;
  }

  public int size() {
    return pointsPerFold.length;
  }

  public int[] getPointCounts() {
    return pointsPerFold;
  }
  
  public void rebuildIndices() {
    if (projection == null) {
      RandomGenerator r = RandomManager.getSeededRandom(seed);
      this.projection = new double[dimensions * projectionBits];
      for (int i = 0; i < projection.length; i++) {
        projection[i] = r.nextGaussian();
      }
    }
    indices.clear();
    for (List<RealVector> px : points) {
      List<BitSet> indx = Lists.newArrayList();
      for (RealVector aPx : px) {
        indx.add(index(aPx));
      }
      indices.add(indx);
    }
    updated = false;
  }
  
  public void add(RealVector vec, int centerId) {
    points.get(centerId).add(vec);
    double length = vec.getNorm();
    lengthSquared.get(centerId).add(length * length);
    pointsPerFold[centerId]++;
    updated = true;
  }
  
  private BitSet index(RealVector vec) {
    final double[] prod = new double[projectionBits];

    vec.walkInDefaultOrder(new AbstractRealVectorPreservingVisitor() {
      @Override
      public void visit(int index, double value) {
        for (int j = 0; j < projectionBits; j++) {
          prod[j] += value * projection[index + j * dimensions];
        }
      }
    });

    BitSet bitset = new BitSet(projectionBits);
    for (int i = 0; i < projectionBits; i++) {
      if (prod[i] > 0.0) {
        bitset.set(i);
      }
    }
    return bitset;
  }

  public Distance[] getDistances(RealVector vec, boolean approx) {
    Distance[] distances = new Distance[size()];
    for (int i = 0; i < distances.length; i++) {
      distances[i] = getDistance(vec, i, approx);
    }
    return distances;
  }

  public Distance getDistance(RealVector vec, int id, boolean approx) {
    double distance = Double.POSITIVE_INFINITY;
    int closestPoint = -1;
    if (approx) {
      if (updated) {
        rebuildIndices();
      }

      BitSet q = index(vec);
      List<BitSet> index = indices.get(id);
      SortedSet<Idx> lookup = Sets.newTreeSet();
      for (int j = 0; j < index.size(); j++) {
        Idx idx = new Idx(hammingDistance(q, index.get(j)), j);
        if (lookup.size() < projectionSamples) {
          lookup.add(idx);
        } else if (idx.compareTo(lookup.last()) < 0) {
          lookup.add(idx);
          lookup.remove(lookup.last());
        }
      }

      List<RealVector> p = points.get(id);
      List<Double> lsq = lengthSquared.get(id);
      for (Idx idx : lookup) {
        double lenSq = lsq.get(idx.getIndex());
        double length = vec.getNorm();
        double d = length * length + lenSq - 2 * vec.dotProduct(p.get(idx.getIndex()));
        if (d < distance) {
          distance = d;
          closestPoint = idx.getIndex();
        }
      }
    } else { // More expensive exact computation
      List<RealVector> px = points.get(id);
      List<Double> lsq = lengthSquared.get(id);
      for (int j = 0; j < px.size(); j++) {
        RealVector p = px.get(j);
        double lenSq = lsq.get(j);
        double length = vec.getNorm();
        double d = length * length + lenSq - 2 * vec.dotProduct(p);
        if (d < distance) {
          distance = d;
          closestPoint = j;
        }
      }
    }
    
    return new Distance(distance, closestPoint);
  }
  
  static final class Idx implements Comparable<Idx> {
    private final int distance;
    private final int index;
    
    Idx(int distance, int index) {
      this.distance = distance;
      this.index = index;
    }

    int getIndex() {
      return index;
    }

    @Override
    public int compareTo(Idx idx) {
      if (distance < idx.distance) {
        return -1;
      }
      if (distance > idx.distance) {
        return 1;
      }
      return 0;
    }
    
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Idx)) {
        return false;
      }
      Idx other = (Idx) o;
      return distance == other.distance && index == other.index;
    }
    
    @Override
    public int hashCode() {
      return distance ^ index;
    }
  }
  
  private static int hammingDistance(BitSet q, BitSet idx) {
    BitSet x = new BitSet(q.size());
    x.or(q);
    x.xor(idx);
    return x.cardinality();
  }

  public List<WeightedRealVector> getWeightedVectorsForFold(int foldId, long[] weights) {
    List<WeightedRealVector> ret = Lists.newArrayList();
    int i = 0;
    for (RealVector vec : points.get(foldId)) {
      ret.add(new WeightedRealVector(vec, weights[i]));
      i++;
    }
    return ret;
  }

  public List<List<WeightedRealVector>> getWeightedVectors(ClosestSketchVectorData data) {
    List<List<WeightedRealVector>> ret = Lists.newArrayList();
    for (int i = 0; i < data.getNumFolds(); i++) {
      List<RealVector> p = points.get(i);
      List<WeightedRealVector> weighted = Lists.newArrayList();
      for (int j = 0; j < p.size(); j++) { // TODO: Assume static? Or fold specific? Or something?
        weighted.add(new WeightedRealVector(p.get(j), data.get(i, j)));
      }
      ret.add(weighted);
    }
    return ret;
  }
  
}
