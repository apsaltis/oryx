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

import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.kmeans.common.pmml.KMeansPMML;

import org.apache.commons.math3.linear.RealVector;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public final class CentersIndexLoader implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(CentersIndexLoader.class);

  private final ClusterSettings clusterSettings;

  public CentersIndexLoader(ClusterSettings settings) {
    this.clusterSettings = settings;
  }

  public KSketchIndex load(String prefix) throws IOException {
    // Check config for hard-coded options or evaluation criteria
    return getIndex(prefix + "model.pmml.gz");
  }

  private KSketchIndex getIndex(String modelsFile) throws IOException {
    Store store = Store.get();
    try {
      PMML pmml = KMeansPMML.read(store.streamFrom(modelsFile));
      List<Model> models = pmml.getModels();
      KSketchIndex index = new KSketchIndex(models.size(), getDimension(models),
          clusterSettings.getIndexBits(), clusterSettings.getIndexSamples(), 1729L);
      int modelId = 0;
      for (Model m : models) {
        ClusteringModel cm = (ClusteringModel) m;
        for (RealVector v : KMeansPMML.toCenters(cm)) {
          index.add(v, modelId);
        }
        modelId++;
      }
      return index;
    } catch (JAXBException e) {
      throw new IOException("JAXB serialization error", e);
    } catch (SAXException e) {
      throw new IOException("SAX serialization error", e);
    }
  }

  private static int getDimension(List<Model> models) {
    return ((ClusteringModel) models.get(0)).getClusteringFields().size();
  }
}
