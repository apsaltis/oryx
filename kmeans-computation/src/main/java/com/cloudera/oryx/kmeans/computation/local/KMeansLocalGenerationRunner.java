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

package com.cloudera.oryx.kmeans.computation.local;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.JobException;
import com.cloudera.oryx.computation.common.LocalGenerationRunner;
import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.common.pmml.KMeansPMML;
import com.cloudera.oryx.kmeans.computation.evaluate.KMeansEvaluationData;
import com.cloudera.oryx.kmeans.computation.pmml.ClusteringModelBuilder;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.math3.linear.RealVector;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.Model;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class KMeansLocalGenerationRunner extends LocalGenerationRunner {
  @Override
  protected void runSteps() throws IOException, InterruptedException, JobException {
    String instanceDir = getInstanceDir();
    long generationID = getGenerationID();
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);

    File currentInboundDir = Files.createTempDir();
    currentInboundDir.deleteOnExit();
    File tempOutDir = Files.createTempDir();
    tempOutDir.deleteOnExit();

    try {
      Store store = Store.get();
      store.downloadDirectory(generationPrefix + "inbound/", currentInboundDir);
      Summary summary = new Summarize(currentInboundDir).call();
      List<List<RealVector>> foldVecs = new Standarize(currentInboundDir, summary).call();
      List<List<WeightedRealVector>> weighted = new WeightedPointsByFold(foldVecs).call();
      List<KMeansEvaluationData> evalData = new ClusteringEvaluation(weighted).call();
      ClusteringModelBuilder b = new ClusteringModelBuilder(summary);
      DataDictionary dictionary = b.getDictionary();
      List<Model> models = Lists.newArrayList();
      List<String> stats = Lists.newArrayList();
      for (KMeansEvaluationData data : evalData) {
        stats.add(data.getClusterValidityStatistics().toString());
        ClusteringModel cm = b.build(data.getName(generationPrefix), data.getBest());
        models.add(cm);
      }
      Files.write(Joiner.on("\n").join(stats) + '\n', new File(tempOutDir, "cluster_stats.csv"), Charsets.UTF_8);
      KMeansPMML.write(new File(tempOutDir, "model.pmml.gz"), dictionary, models);
      store.uploadDirectory(generationPrefix, tempOutDir, false);
    } catch (ExecutionException ee) {
      throw new JobException(ee.getCause());
    } finally {
      IOUtils.deleteRecursively(tempOutDir);
    }
  }
}
