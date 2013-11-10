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

import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.computation.common.sample.ReservoirSampling;
import com.cloudera.oryx.computation.common.types.Serializables;
import com.cloudera.oryx.kmeans.computation.KMeansJobStep;
import com.cloudera.oryx.kmeans.computation.MLAvros;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

import java.io.IOException;

public final class KSketchSamplingStep extends KMeansJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {
    JobStepConfig stepConfig = getConfig();
    ClusterSettings settings = ClusterSettings.create(ConfigUtils.getDefaultConfig());

    String instanceDir = stepConfig.getInstanceDir();
    long generationID = stepConfig.getGenerationID();
    int iteration = stepConfig.getIteration();
    String prefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String outputKey = prefix + String.format("sketch/%d/", iteration);
    if (!validOutputPath(outputKey)) {
      return null;
    }

    // get normalized vectors
    String inputKey = prefix + "normalized/";
    MRPipeline p = createBasicPipeline(DistanceToClosestFn.class);
    AvroType<Pair<Integer, RealVector>> inputType = Avros.pairs(Avros.ints(), MLAvros.vector());
    PCollection<Pair<Integer, RealVector>> in = p.read(avroInput(inputKey, inputType));

    // either create or load the set of currently chosen k-sketch vectors
    // they are stored in a KSketchIndex object
    DistanceToClosestFn<RealVector> distanceToClosestFn;
    UpdateIndexFn updateIndexFn;
    if (iteration == 1) { // Iteration 1 is the first real iteration; iteration 0 contains initial state
      KSketchIndex index = createInitialIndex(settings, in);
      distanceToClosestFn = new DistanceToClosestFn<RealVector>(index);
      updateIndexFn = new UpdateIndexFn(index);
    } else {
      // Get the index location from the previous iteration
      String previousIndexKey = prefix + String.format("sketch/%d/", iteration - 1);
      distanceToClosestFn = new DistanceToClosestFn<RealVector>(previousIndexKey);
      updateIndexFn = new UpdateIndexFn(previousIndexKey);
    }

    // compute distance of each vector in dataset to closest vector in k-sketch
    PTable<Integer, Pair<RealVector, Double>> weighted = in.parallelDo("computeDistances", distanceToClosestFn,
        Avros.tableOf(Avros.ints(), Avros.pairs(MLAvros.vector(), Avros.doubles())));

    // run weighted reservoir sampling on the vector to select another group of settings.getSketchPoints()
    // to add to the k-sketch
    PTable<Integer,RealVector> kSketchSample = ReservoirSampling.groupedWeightedSample(weighted,
        settings.getSketchPoints(), RandomManager.getRandom());

    // update the KSketchIndex with the newly-chosen vectors
    kSketchSample.parallelDo("updateIndex", updateIndexFn, Serializables.avro(KSketchIndex.class))
        .write(avroOutput(outputKey));

    return p;
  }

  public static KSketchIndex createInitialIndex(ClusterSettings settings,
                                                 PCollection<Pair<Integer, RealVector>> input) {
    RealVector[] init = new RealVector[settings.getCrossFolds()];
    for (Pair<Integer, RealVector> rv : input.materialize()) {
      if (init[rv.first()] == null) {
        init[rv.first()] = rv.second();
      }
      boolean done = true;
      for (RealVector vec : init) {
        if (vec == null) {
          done = false;
          break;
        }
      }
      if (done) {
        break;
      }
    }

    KSketchIndex index = new KSketchIndex(
        settings.getCrossFolds(),
        init[0].getDimension(),
        settings.getIndexBits(),
        settings.getIndexSamples(),
        1729L); // TODO: something smarter, or figure out that I don't need this b/c I compute the projections up front
    for (int i = 0; i < init.length; i++) {
      index.add(init[i], i);
    }
    return index;
  }
}