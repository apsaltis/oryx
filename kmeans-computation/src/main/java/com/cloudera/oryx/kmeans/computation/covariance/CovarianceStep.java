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

package com.cloudera.oryx.kmeans.computation.covariance;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.kmeans.computation.KMeansJobStep;
import com.cloudera.oryx.kmeans.computation.MLAvros;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;

import java.io.IOException;

public final class CovarianceStep extends KMeansJobStep {
  private static final PType<Index> INDEX_PTYPE = Avros.reflects(Index.class);
  private static final PType<CoMoment> COMOMENT_PTYPE = Avros.reflects(CoMoment.class);
  private static final PType<ClusterKey> CKEY_PTYPE = Avros.reflects(ClusterKey.class);

  @Override
  protected MRPipeline createPipeline() throws IOException {
    JobStepConfig stepConfig = getConfig();
    CovarianceSettings settings = CovarianceSettings.create();

    String instanceDir = stepConfig.getInstanceDir();
    long generationID = stepConfig.getGenerationID();
    String prefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String outputKey = prefix + "covariance/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    KSketchIndex index = getCentersIndex(prefix);

    String inputKey = prefix + "normalized/";
    MRPipeline p = createBasicPipeline(CoMomentKeyFn.class);
    inputVectors(p, inputKey, MLAvros.vector()).parallelDo(
        "covAssign",
        new AssignFn<RealVector>(index, settings.useApprox()),
        Avros.tableOf(CKEY_PTYPE, Avros.pairs(MLAvros.vector(), Avros.doubles())))
        .parallelDo(
            "coMoment",
            new CoMomentKeyFn<ClusterKey>(CKEY_PTYPE),
            Avros.tableOf(Avros.pairs(CKEY_PTYPE, INDEX_PTYPE), COMOMENT_PTYPE))
        .groupByKey()
        .combineValues(new CoMomentAggregator())
        .parallelDo("covData", new CovarianceDataStringFn(), Writables.strings())
        .write(compressedTextOutput(p.getConfiguration(), outputKey));

    return p;
  }

}