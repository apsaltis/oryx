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

package com.cloudera.oryx.kmeans.computation;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.computation.common.fn.StringSplitFn;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import com.cloudera.oryx.kmeans.computation.cluster.CentersIndexLoader;
import com.cloudera.oryx.kmeans.computation.cluster.ClusterSettings;
import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.lib.PTables;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;

import java.io.IOException;

public class KMeansJobStep extends JobStep {

  @Override
  protected final KMeansJobStepConfig getConfig() {
    return (KMeansJobStepConfig) super.getConfig();
  }

  @Override
  protected final JobStepConfig parseConfig(String[] args) {
    Preconditions.checkNotNull(args);
    Preconditions.checkArgument(args.length >= 4);
    return new KMeansJobStepConfig(args[0],
        Long.parseLong(args[1]),
        Long.parseLong(args[2]),
        Integer.parseInt(args[3]));
  }

  protected final <V extends RealVector> PCollection<Pair<Integer, V>> inputPairs(
      Pipeline p,
      String inputKey,
      PType<V> ptype) {
    PType<Pair<Integer, V>> inputType = Avros.pairs(Avros.ints(), ptype);
    return p.read(avroInput(inputKey, inputType));
  }

  protected final <V extends RealVector> PCollection<V> inputVectors(Pipeline p, String inputKey, PType<V> ptype) {
    return PTables.asPTable(inputPairs(p, inputKey, ptype)).values();
  }

  protected final KSketchIndex getCentersIndex(String prefix) throws IOException {
    return (new CentersIndexLoader(ClusterSettings.create(ConfigUtils.getDefaultConfig()))).load(prefix);
  }

  protected static PCollection<Record> toRecords(PCollection<String> lines) {
    return StringSplitFn.apply(lines);
  }
}
