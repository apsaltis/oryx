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

package com.cloudera.oryx.kmeans.serving.generation;

import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kmeans.common.pmml.KMeansPMML;
import com.cloudera.oryx.serving.generation.GenerationManager;

public final class KMeansGenerationManager extends GenerationManager {

  private static final Logger log = LoggerFactory.getLogger(KMeansGenerationManager.class);

  private long modelGeneration;
  private Generation currentGeneration;

  public KMeansGenerationManager(File appendTempDir) throws IOException {
    super(appendTempDir);
    modelGeneration = NO_GENERATION;
  }

  /**
   * @return an instance of the latest {@link Generation} that has been made available by the
   * implementation.
   */
  public Generation getCurrentGeneration() {
    return currentGeneration;
  }

  @Override
  protected void loadRecentModel(long mostRecentModelGeneration) throws IOException {
    if (mostRecentModelGeneration <= modelGeneration) {
      return;
    }
    if (modelGeneration == NO_GENERATION) {
      log.info("Most recent generation {} is the first available one", mostRecentModelGeneration);
    } else {
      log.info("Most recent generation {} is newer than current {}", mostRecentModelGeneration, modelGeneration);
    }

    File modelPMMLFile = File.createTempFile("model-", ".pmml.gz");
    modelPMMLFile.deleteOnExit();
    IOUtils.delete(modelPMMLFile);

    String instanceDir = ConfigUtils.getDefaultConfig().getString("model.instance-dir");

    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, mostRecentModelGeneration);
    String modelPMMLKey = generationPrefix + "model.pmml.gz";
    Store.get().download(modelPMMLKey, modelPMMLFile);
    log.info("Loading model description from {}", modelPMMLKey);

    PMML pmmlModel = KMeansPMML.read(modelPMMLFile);

    modelGeneration = mostRecentModelGeneration;
    //TODO: handle multi-cluster case
    currentGeneration = new Generation((ClusteringModel) pmmlModel.getModels().get(0));
  }

  public synchronized void append(CharSequence example) throws IOException {
    Writer appender = getAppender();
    if (appender != null) {
      appender.append(example + "\n");
    }
    decrementCountdownToUpload();
  }

}
