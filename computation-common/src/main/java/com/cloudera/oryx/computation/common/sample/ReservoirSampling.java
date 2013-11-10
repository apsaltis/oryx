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

package com.cloudera.oryx.computation.common.sample;

import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.collect.Maps;

import com.cloudera.oryx.common.random.RandomManager;

/**
 * Generates a weighted random sample of N items from a distributed data set using the reservoir
 * algorithm described in <a href="http://utopia.duth.gr/~pefraimi/research/data/2007EncOfAlg.pdf">Efraimidis
 * and Spirakis (2005)</a>.
 * 
 */
public final class ReservoirSampling {

  private ReservoirSampling() {
  }

  public static <T> PCollection<T> sample(
      PCollection<T> input,
      int sampleSize) {
    return sample(input, sampleSize, null);
  }

  public static <T> PCollection<T> sample(
      PCollection<T> input,
      int sampleSize,
      RandomGenerator random) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<Pair<T, Integer>> ptype = ptf.pairs(input.getPType(), ptf.ints());
    return weightedSample(
        input.parallelDo(new MapFn<T, Pair<T, Integer>>() {
          @Override
          public Pair<T, Integer> map(T t) { return Pair.of(t, 1); }
        }, ptype),
        sampleSize,
        random);
  }
  
  public static <T, N extends Number> PCollection<T> weightedSample(
      PCollection<Pair<T, N>> input,
      int sampleSize) {
    return weightedSample(input, sampleSize, null);
  }
  
  public static <T, N extends Number> PCollection<T> weightedSample(
      PCollection<Pair<T, N>> input,
      int sampleSize,
      RandomGenerator random) {
    PTypeFamily ptf = input.getTypeFamily();
    PTable<Integer, Pair<T, N>> groupedIn = input.parallelDo(
        new MapFn<Pair<T, N>, Pair<Integer, Pair<T, N>>>() {
          @Override
          public Pair<Integer, Pair<T, N>> map(Pair<T, N> p) {
            return Pair.of(0, p);
          }
        }, ptf.tableOf(ptf.ints(), input.getPType()));
    return groupedWeightedSample(groupedIn, sampleSize, random).values();
  }
  
  public static <K, T, N extends Number> PTable<K, T> groupedWeightedSample(
      PTable<K, Pair<T, N>> input,
      int sampleSize) {
    return groupedWeightedSample(input, sampleSize, null);
  }
  
  public static <K, T, N extends Number> PTable<K, T> groupedWeightedSample(
      PTable<K, Pair<T, N>> input,
      int sampleSize,
      RandomGenerator random) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<K> keyType = input.getPTableType().getKeyType();
    @SuppressWarnings("unchecked")
    PType<T> ttype = (PType<T>) input.getPTableType().getValueType().getSubTypes().get(0);
    PTableType<K, Pair<Double, T>> ptt = ptf.tableOf(keyType, ptf.pairs(ptf.doubles(), ttype));

    // fill reservoirs by mapping over the vectors and re-emiting them; each map task emits at most sampleSize
    // vectors per fold; the combiner/reducer will combine the outputs and pare down to sampleSize vectors total
    PTable<K, Pair<Double, T>> samples = input.parallelDo("reservoirSampling",
        new SampleFn<K, T, N>(sampleSize, random, ttype), ptt);

    // pare down to just a single reservoir with sampleSize vectors
    PTable<K, Pair<Double, T>> reservoir = samples.groupByKey(1).combineValues(new WRSCombineFn<K, T>(sampleSize, ttype));

    // strip the weights off the final sampled reservoir and return
    return reservoir.parallelDo("strippingSamplingWeights", new MapFn<Pair<K, Pair<Double, T>>, Pair<K, T>>() {
      @Override
      public Pair<K, T> map(Pair<K, Pair<Double, T>> p) {
        return Pair.of(p.first(), p.second().second());
      }
    }, ptf.tableOf(keyType, ttype));
  }

  private static final class SampleFn<K, T, N extends Number>
      extends DoFn<Pair<K, Pair<T, N>>, Pair<K, Pair<Double, T>>> {
  
    private static final int PUT_LIMIT = 100000;
    
    private final int sampleSize;
    private final PType<T> ptype;
    private transient Map<K, SortedMap<Double, T>> current;
    private RandomGenerator random;
    private int puts;
    
    private SampleFn(int sampleSize, RandomGenerator random, PType<T> ptype) {
      this.sampleSize = sampleSize;
      this.ptype = ptype;
      this.random = random;
    }
    
    @Override
    public float scaleFactor() {
      // Indicate that the output of sampling will be on the small side
      return 0.05f;
    }
    
    @Override
    public void initialize() {
      ptype.initialize(getConfiguration());
      if (current == null) {
        this.current = Maps.newHashMap();
      } else {
        current.clear();
      }
      if (random == null) {
        this.random = RandomManager.getRandom();
      }
    }
    
    @Override
    public void process(Pair<K, Pair<T, N>> input,
        Emitter<Pair<K, Pair<Double, T>>> emitter) {
      K id = input.first();
      Pair<T, N> p = input.second();
      double weight = p.second().doubleValue();
      if (weight > 0.0) {
        double score = Math.log(random.nextDouble()) / weight;
        SortedMap<Double, T> reservoir = current.get(id);
        if (reservoir == null) {
          reservoir = Maps.newTreeMap();
          current.put(id, reservoir);
        }
        if (reservoir.size() < sampleSize) { 
          reservoir.put(score, ptype.getDetachedValue(p.first()));
          puts++;
        } else if (score > reservoir.firstKey()) {
          reservoir.remove(reservoir.firstKey());
          reservoir.put(score, ptype.getDetachedValue(p.first()));
        }
        if (puts > PUT_LIMIT) {
          // On the off-chance this gets huge, cleanup
          // Can occur if sampleSize > PUT_LIMIT
          cleanup(emitter);
        }
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<K, Pair<Double, T>>> emitter) {
      for (Map.Entry<K,SortedMap<Double,T>> entry : current.entrySet()) {
        Map<Double, T> reservoir = entry.getValue();
        for (Map.Entry<Double,T> e : reservoir.entrySet()) {
          emitter.emit(Pair.of(entry.getKey(), Pair.of(e.getKey(), e.getValue())));
        }
      }
      current.clear();
      puts = 0;
    }
  }
  
  private static final class WRSCombineFn<K, T> extends CombineFn<K, Pair<Double, T>> {

    private final int sampleSize;
    private final PType<T> ptype;
    
    private WRSCombineFn(int sampleSize, PType<T> ptype) {
      this.sampleSize = sampleSize;
      this.ptype = ptype;
    }

    @Override
    public void initialize() {
      ptype.initialize(getConfiguration());
    }
    
    @Override
    public void process(Pair<K, Iterable<Pair<Double, T>>> input,
        Emitter<Pair<K, Pair<Double, T>>> emitter) {
      SortedMap<Double, T> reservoir = Maps.newTreeMap();
      for (Pair<Double, T> p : input.second()) {
        if (reservoir.size() < sampleSize) { 
          reservoir.put(p.first(), ptype.getDetachedValue(p.second()));        
        } else if (p.first() > reservoir.firstKey()) {
          reservoir.remove(reservoir.firstKey());
          reservoir.put(p.first(), ptype.getDetachedValue(p.second()));  
        }
      }
      for (Map.Entry<Double, T> e : reservoir.entrySet()) {
        emitter.emit(Pair.of(input.first(), Pair.of(e.getKey(), e.getValue())));
      }
    }
  }
}
