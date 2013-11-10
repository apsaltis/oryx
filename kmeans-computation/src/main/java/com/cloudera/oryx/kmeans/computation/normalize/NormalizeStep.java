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

package com.cloudera.oryx.kmeans.computation.normalize;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.computation.common.crossfold.Crossfold;
import com.cloudera.oryx.computation.common.fn.StringSplitFn;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.kmeans.computation.KMeansJobStep;
import com.cloudera.oryx.kmeans.computation.MLAvros;

import com.typesafe.config.Config;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;

import java.io.IOException;

public final class NormalizeStep extends KMeansJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {
    JobStepConfig stepConfig = getConfig();

    String instanceDir = stepConfig.getInstanceDir();
    long generationID = stepConfig.getGenerationID();
    String prefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String outputKey = prefix + "normalized/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    String inboundKey = prefix + "inbound/";
    String summaryKey = prefix + "summary/";
    Config config = ConfigUtils.getDefaultConfig();
    InboundSettings inbound = InboundSettings.create(config);
    NormalizeSettings settings = NormalizeSettings.create(config);

    MRPipeline p = createBasicPipeline(StringSplitFn.class);
    PCollection<Record> records = toRecords(p.read(textInput(inboundKey)));
    StandardizeFn standardizeFn = getStandardizeFn(inbound, settings, summaryKey);
    PCollection<RealVector> vecs = records.parallelDo("normalize", standardizeFn, MLAvros.vector());

    // assign cross-folds here
    new Crossfold(config.getInt("model.cross-folds"))
        .apply(vecs)
        .write(avroOutput(outputKey));

    return p;
  }

  static StandardizeFn getStandardizeFn(InboundSettings inbound, NormalizeSettings settings, String summaryKey) {
    return new StandardizeFn(settings, inbound.getIgnoredColumns(), inbound.getIdColumns(),
        summaryKey);
  }
}
