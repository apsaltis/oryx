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

package com.cloudera.oryx.kmeans.computation.outlier;

import com.cloudera.oryx.common.math.NamedRealVector;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.RecordSpec;
import com.cloudera.oryx.computation.common.records.Spec;
import com.cloudera.oryx.computation.common.types.MLRecords;
import com.cloudera.oryx.kmeans.computation.KMeansJobStep;
import com.cloudera.oryx.kmeans.computation.MLAvros;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import com.cloudera.oryx.kmeans.computation.covariance.AssignFn;
import com.cloudera.oryx.kmeans.computation.covariance.ClusterKey;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;

import java.io.IOException;

public final class OutlierStep extends KMeansJobStep {
  @Override
  protected MRPipeline createPipeline() throws IOException {
    JobStepConfig stepConfig = getConfig();

    OutlierSettings settings = OutlierSettings.create();
    String instanceDir = stepConfig.getInstanceDir();
    long generationID = stepConfig.getGenerationID();
    String prefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String outputKey = prefix + "outliers/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    String covarianceKey = prefix + "covariance/";
    if (!validOutputPath(covarianceKey)) {
      covarianceKey = null;
    }

    // For the output data
    RecordSpec.Builder rsb = RecordSpec.builder()
        .add("vector_id", DataType.STRING)
        .add("k", DataType.INT)
        .add("closest_center_id", DataType.INT)
        .add("center_distance", DataType.DOUBLE);
    if (covarianceKey != null) {
      rsb.add("euclidean_distance", DataType.DOUBLE);
      rsb.add("outlier_distance", DataType.DOUBLE);
    }
    Spec spec = rsb.build();

    KSketchIndex index = getCentersIndex(prefix);
    int dim = index.getDimension();

    String inputKey = prefix + "normalized/";
    MRPipeline p = createBasicPipeline(OutlierScoreFn.class);
    inputVectors(p, inputKey, MLAvros.namedVector())
        .parallelDo(
            "assign",
             new AssignFn<NamedRealVector>(index, settings.useApprox()),
             Avros.tableOf(Avros.reflects(ClusterKey.class), Avros.pairs(MLAvros.namedVector(), Avros.doubles())))
        .parallelDo(
            "toRecords",
            new OutlierScoreFn(spec, covarianceKey, dim),
            MLRecords.csvRecord(AvroTypeFamily.getInstance(), String.valueOf(DelimitedDataUtils.DELIMITER)))
        .write(compressedTextOutput(p.getConfiguration(), outputKey));

    return p;
  }
}
