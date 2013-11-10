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

package com.cloudera.oryx.kmeans.computation.evaluate;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.kmeans.computation.KMeansJobStep;
import com.cloudera.oryx.kmeans.computation.MLAvros;
import com.cloudera.oryx.kmeans.computation.cluster.ClusterSettings;
import com.cloudera.oryx.kmeans.computation.types.KMeansTypes;
import com.typesafe.config.Config;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.avro.Avros;

import java.io.IOException;

public final class VoronoiPartitionStep extends KMeansJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {
    JobStepConfig stepConfig = getConfig();
    Config config = ConfigUtils.getDefaultConfig();
    ClusterSettings clusterSettings = ClusterSettings.create(config);

    String instanceDir = stepConfig.getInstanceDir();
    long generationID = stepConfig.getGenerationID();
    String prefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String outputKey = prefix + "weighted/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    String indexKey = prefix + "sketch/" + clusterSettings.getSketchIterations();
    String inputKey = prefix + "normalized/";
    MRPipeline p = createBasicPipeline(ClosestSketchVectorFn.class);

    // first I compute the weight of each k-sketch vector, i.e., Voronoi partition
    // I aggregate all together and persist on disk
    // PCollection<ClosestSketchVectorData> weights = inputPairs(p, inputKey, MLAvros.vector())
    PCollection<ClosestSketchVectorData> weights = PTables.asPTable(inputPairs(p, inputKey, MLAvros.vector())
        .parallelDo("computingSketchVectorWeights",
            new ClosestSketchVectorFn<RealVector>(indexKey, clusterSettings),
            Avros.pairs(Avros.ints(), Avros.reflects(ClosestSketchVectorData.class))))
        .groupByKey(1)
        .combineValues(new ClosestSketchVectorAggregator(clusterSettings))
        .values()
        .write(avroOutput(outputKey + "kSketchVectorWeights/"));

    // this "pipeline" takes a single ClosestSketchVectorData and returns weighted vectors
    // could be done outside MapReduce, but that would require me to materialize the ClosestSketchVectorData
    weights.parallelDo(
        "generatingWeightedSketchVectors",
        new WeightVectorsFn(indexKey),
        KMeansTypes.FOLD_WEIGHTED_VECTOR)
        .write(avroOutput(outputKey + "weightedKSketchVectors/"));

    return p;
  }
}
