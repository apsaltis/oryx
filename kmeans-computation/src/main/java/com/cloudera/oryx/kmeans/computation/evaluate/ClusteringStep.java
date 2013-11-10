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

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.computation.common.types.Serializables;
import com.cloudera.oryx.kmeans.common.ClusterValidityStatistics;
import com.cloudera.oryx.kmeans.common.KMeansEvalStrategy;
import com.cloudera.oryx.kmeans.common.pmml.KMeansPMML;
import com.cloudera.oryx.kmeans.computation.KMeansJobStep;
import com.cloudera.oryx.kmeans.computation.MLAvros;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.types.KMeansTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.util.List;

public final class ClusteringStep extends KMeansJobStep {

  private static final Logger log = LoggerFactory.getLogger(ClusteringStep.class);

  @Override
  protected MRPipeline createPipeline() throws IOException {
    JobStepConfig stepConfig = getConfig();
    Config config = ConfigUtils.getDefaultConfig();
    EvaluationSettings settings = EvaluationSettings.create(config);

    String instanceDir = stepConfig.getInstanceDir();
    long generationID = stepConfig.getGenerationID();
    String prefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String outputKey = prefix + "eval/";
    if (!validOutputPath(outputKey)) {
      return null;
    }
    String inputKey = prefix + "weighted/weightedKSketchVectors/";
    MRPipeline p = createBasicPipeline(ClosestSketchVectorFn.class);

    PType<Pair<Integer, WeightedRealVector>> inputType = KMeansTypes.FOLD_WEIGHTED_VECTOR;
    PCollection<Pair<Integer, WeightedRealVector>> weightedSketchVectors = p.read(avroInput(inputKey, inputType));

    PCollection<KMeansEvaluationData> evaluation = weightedSketchVectors
        .parallelDo("replicate",
            new ReplicateValuesFn<Pair<Integer, WeightedRealVector>>(settings.getKValues(), settings.getReplications()),
            Avros.tableOf(Avros.pairs(Avros.ints(), Avros.ints()), Avros.pairs(Avros.ints(), MLAvros.weightedVector())))
        .groupByKey(settings.getParallelism())
        .parallelDo("cluster",
            new KMeansClusteringFn(settings),
            Serializables.avro(KMeansEvaluationData.class));

    // Write out the centers themselves to a text file
    evaluation.parallelDo("replicaCenters", new CentersOutputFn(prefix), Avros.strings())
        .write(compressedTextOutput(p.getConfiguration(), outputKey + "replicaCenters/"));

    // Write out the per-replica stats
    evaluation.parallelDo("replicaStats", new StatsOutputFn(), Avros.strings())
        .write(compressedTextOutput(p.getConfiguration(), outputKey + "replicaStats/"));

    return p;
  }

  @Override
  protected void postRun() throws IOException {
    JobStepConfig stepConfig = getConfig();
    String instanceDir = stepConfig.getInstanceDir();
    long generationID = stepConfig.getGenerationID();
    String prefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String replicaStatsKey = prefix + "eval/replicaStats/";
    String replicaCentersKey = prefix + "eval/replicaCenters/";

    boolean succeeded = false;
    try {
      List<ClusterValidityStatistics> stats = findBest(replicaStatsKey);
      uploadCenters(prefix, replicaCentersKey, stats);
      succeeded = true;
    } finally {
      if (!succeeded) {
        // This causes a re-run of the M/R next time if failed
        Store.get().delete(prefix + "_SUCCESS");
      }
    }
  }

  private static List<ClusterValidityStatistics> findBest(String replicaStatsKey) throws IOException {
    KMeansEvalStrategy evalStrategy = EvaluationSettings.create(ConfigUtils.getDefaultConfig()).getEvalStrategy();
    Store store = Store.get();
    List<ClusterValidityStatistics> stats = Lists.newArrayList();
    for (String statsKey : store.list(replicaStatsKey, true)) {
      for (String line : new FileLineIterable(store.readFrom(statsKey))) {
        ClusterValidityStatistics cvs = ClusterValidityStatistics.parse(line);
        log.info("PS: {}", cvs);
        stats.add(cvs);
      }
    }
    return evalStrategy == null ? stats : evalStrategy.evaluate(stats);
  }

  private static void uploadCenters(String prefix,
                                    String replicaCentersKey,
                                    List<ClusterValidityStatistics> bestStats) throws IOException {
    Store store = Store.get();
    List<Model> models = Lists.newArrayList();
    String bestName = null;
    if (bestStats.size() == 1) {
      ClusterValidityStatistics cvs = bestStats.get(0);
      bestName = prefix + ':' + cvs.getK() + ':' + cvs.getReplica();
      log.info("Best scoring model = {}", bestName);
    }
    log.info("Reading model centers from key = {}", replicaCentersKey);
    DataDictionary dictionary = null;
    for (String modelKey : store.list(replicaCentersKey, true)) {
      try {
        PMML pmml = KMeansPMML.read(store.streamFrom(modelKey));
        log.info("Read {} from key = {}", pmml.getModels().size(), modelKey);
        if (dictionary == null) {
          dictionary = pmml.getDataDictionary();
        }
        if (bestName == null) {
          models.addAll(pmml.getModels());
        } else {
          for (Model m : pmml.getModels()) {
            if (bestName.equals(m.getModelName())) {
              models = ImmutableList.of(m);
              break;
            }
          }
        }
      } catch (JAXBException e) {
        log.error("Serialization error", e);
      } catch (SAXException e) {
        log.error("Serialization error", e);
      }
    }

    File keptModels = File.createTempFile("model", ".pmml.gz");
    keptModels.deleteOnExit();
    log.info("Writing {} models to model.pmml.gz...", models.size());
    KMeansPMML.write(keptModels, dictionary, models);
    store.upload(prefix + "model.pmml.gz", keptModels, true);
    IOUtils.delete(keptModels);
  }
}
