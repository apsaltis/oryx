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

package com.cloudera.oryx.rdf.computation;

import org.dmg.pmml.IOUtil;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.PMML;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.DependsOn;
import com.cloudera.oryx.computation.common.DistributedGenerationRunner;
import com.cloudera.oryx.computation.common.JobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.rdf.computation.build.BuildTreesStep;
import com.cloudera.oryx.rdf.computation.build.MergeNewOldStep;

/**
 * @author Sean Owen
 */
public final class RDFDistributedGenerationRunner extends DistributedGenerationRunner {

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPreDependencies() {
    return Collections.emptyList();
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getIterationDependencies() {
    return Collections.singletonList(
        DependsOn.<Class<? extends JobStep>>nextAfterFirst(BuildTreesStep.class, MergeNewOldStep.class));
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPostDependencies() {
    return Collections.emptyList();
  }

  @Override
  protected JobStepConfig buildConfig(int iteration) {
    return new RDFJobStepConfig(getInstanceDir(),
                                getGenerationID(),
                                getLastGenerationID(),
                                iteration);
  }

  @Override
  protected void doPost() throws IOException {

    String instanceGenerationPrefix =
        Namespaces.getInstanceGenerationPrefix(getInstanceDir(), getGenerationID());
    String outputPathKey = instanceGenerationPrefix + "trees/";
    Store store = Store.get();
    PMML joinedForest = null;

    for (String treePrefix : store.list(outputPathKey, true)) {

      File treeTempFile = File.createTempFile("model-", ".pmml.gz");
      treeTempFile.deleteOnExit();
      store.download(treePrefix, treeTempFile);

      PMML pmml;
      InputStream in = IOUtils.openMaybeDecompressing(treeTempFile);
      try {
        pmml = IOUtil.unmarshal(in);
      } catch (SAXException e) {
        throw new IOException(e);
      } catch (JAXBException e) {
        throw new IOException(e);
      } finally {
        in.close();
      }

      IOUtils.delete(treeTempFile);

      if (joinedForest == null) {
        joinedForest = pmml;
      } else {
        MiningModel existingModel = (MiningModel) joinedForest.getModels().get(0);
        MiningModel nextModel = (MiningModel) pmml.getModels().get(0);
        existingModel.getSegmentation().getSegments().addAll(nextModel.getSegmentation().getSegments());
      }

    }

    File tempJoinedForestFile = File.createTempFile("model-", ".pmml.gz");
    tempJoinedForestFile.deleteOnExit();
    OutputStream out = IOUtils.buildGZIPOutputStream(new FileOutputStream(tempJoinedForestFile));
    try {
      IOUtil.marshal(joinedForest, out);
    } catch (JAXBException e) {
      throw new IOException(e);
    } finally {
      out.close();
    }

    store.upload(instanceGenerationPrefix + "model.pmml.gz", tempJoinedForestFile, false);
    IOUtils.delete(tempJoinedForestFile);
  }

  @Override
  protected boolean areIterationsDone(int iterationNumber) {
    return true;
  }

}
