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

import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;
import com.cloudera.oryx.computation.common.json.JacksonUtils;
import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.kmeans.common.pmml.KMeansPMML;
import com.cloudera.oryx.kmeans.computation.pmml.ClusteringModelBuilder;
import com.google.common.collect.Lists;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.dmg.pmml.Model;

import javax.xml.bind.JAXBException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public final class CentersOutputFn extends OryxDoFn<KMeansEvaluationData, String> {

  private final String prefix;
  private List<Model> models;
  private transient ClusteringModelBuilder builder;

  public CentersOutputFn(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public void initialize() {
    Store store = Store.get();
    List<String> pieces;
    try {
      pieces = store.list(prefix + "summary/", true);
    } catch (IOException e) {
      throw new CrunchRuntimeException("Could not read summary key", e);
    }

    if (pieces.size() != 1) {
      throw new IllegalStateException("Expected exactly one summary file: " + pieces);
    }
    Summary summary;
    try {
      summary = JacksonUtils.getObjectMapper().readValue(store.readFrom(pieces.get(0)), Summary.class);
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
    models = Lists.newArrayList();
    builder = new ClusteringModelBuilder(summary);
  }

  @Override
  public void process(KMeansEvaluationData input, Emitter<String> emitter) {
    models.add(builder.build(input.getName(prefix), input.getBest()));
  }

  @Override
  public void cleanup(Emitter<String> emitter) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      KMeansPMML.write(os, builder.getDictionary(), models);
    } catch (JAXBException e) {
      throw new IllegalStateException("PMML serialization error", e);
    }
    try {
      os.close();
    } catch (IOException e) {
      throw new IllegalStateException("Surprising IO error", e);
    }
    emitter.emit(os.toString());
  }
}
