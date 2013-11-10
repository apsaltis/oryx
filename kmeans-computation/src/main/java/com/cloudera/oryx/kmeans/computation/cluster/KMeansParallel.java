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

import java.util.List;

import com.cloudera.oryx.kmeans.common.Distance;
import com.cloudera.oryx.kmeans.common.Centers;
import com.cloudera.oryx.computation.common.fn.SumVectorsAggregator;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.crunch.Aggregator;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.materialize.pobject.PObjectImpl;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.collect.Lists;

public final class KMeansParallel {

  private final int projectionBits;
  private final int projectionSamples;
  private final long seed;

  /**
   * Main constructor that includes the option to uses a fixed {@code Random} instance
   * for running the k-means algorithm for testing purposes.
   */
  public KMeansParallel(RandomGenerator random, int projectionBits, int projectionSamples) {
    this.projectionBits = projectionBits;
    this.projectionSamples = projectionSamples;
    if (random == null) {
      this.seed = System.currentTimeMillis();
    } else {
      this.seed = random.nextLong();
    }
  }

  /**
   * Runs Lloyd's algorithm on the given points for a given number of iterations, returning the final
   * centers that result.
   * 
   * @param points The data points to cluster
   * @param centers The list of initial centers
   * @param numIterations The number of iterations to run, with each iteration corresponding to a MapReduce job
   * @param approx Whether to use random projection for assigning points to centers
   */
  public <V extends RealVector> List<Centers> lloydsAlgorithm(PCollection<V> points, List<Centers> centers,
      int numIterations, boolean approx) {
    PTypeFamily tf = points.getTypeFamily();
    PTableType<Pair<Integer, Integer>, Pair<V, Long>> ptt = tf.tableOf(tf.pairs(tf.ints(), tf.ints()),
        tf.pairs(points.getPType(), tf.longs()));
    Aggregator<Pair<V, Long>> agg = new SumVectorsAggregator<V>();
    for (int i = 0; i < numIterations; i++) {
      KSketchIndex index = new KSketchIndex(centers, projectionBits, projectionSamples, seed);
      LloydsMapFn<V> mapFn = new LloydsMapFn<V>(index, approx);
      centers = new LloydsCenters<V>(points.parallelDo("lloyds-" + i, mapFn, ptt)
          .groupByKey()
          .combineValues(agg), centers.size()).getValue();
    }
    return centers;
  }
  
  private static final class LloydsMapFn<V extends RealVector> extends DoFn<V, Pair<Pair<Integer, Integer>, Pair<V, Long>>> {
    private final KSketchIndex centers;
    private final boolean approx;
    
    private LloydsMapFn(KSketchIndex centers, boolean approx) {
      this.centers = centers;
      this.approx = approx;
    }
    
    @Override
    public void process(V vec, Emitter<Pair<Pair<Integer, Integer>, Pair<V, Long>>> emitFn) {
      Pair<V, Long> out = Pair.of(vec, 1L);
      for (int i = 0; i < centers.size(); i++) {
        Distance d = centers.getDistance(vec, i, approx);
        emitFn.emit(Pair.of(Pair.of(i, d.getClosestCenterId()), out));
      }
    }
  }
  
  private static final class LloydsCenters<V extends RealVector> extends PObjectImpl<Pair<Pair<Integer, Integer>, Pair<V, Long>>, List<Centers>> {

    private final int numCenters;
    
    LloydsCenters(PTable<Pair<Integer, Integer>, Pair<V, Long>> collect, int numCenters) {
      super(collect);
      this.numCenters = numCenters;
    }

    @Override
    protected List<Centers> process(Iterable<Pair<Pair<Integer, Integer>, Pair<V, Long>>> values) {
      List<Centers> centers = Lists.newArrayListWithExpectedSize(numCenters);
      for (int i = 0; i < numCenters; i++) {
        centers.add(new Centers());
      }
      for (Pair<Pair<Integer, Integer>, Pair<V, Long>> p : values) {
        int centerId = p.first().first();
        RealVector c = p.second().first().mapDivide(p.second().second());
        centers.set(centerId, centers.get(centerId).extendWith(c));
      }
      return centers;
    }
  }
}
