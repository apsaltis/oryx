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

package com.cloudera.oryx.kmeans.computation.summary;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.computation.common.fn.StringSplitFn;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.computation.common.records.DataType;
import com.cloudera.oryx.computation.common.records.Record;
import com.cloudera.oryx.computation.common.records.RecordSpec;
import com.cloudera.oryx.computation.common.records.Spec;
import com.cloudera.oryx.computation.common.summary.Summarizer;
import com.cloudera.oryx.kmeans.computation.KMeansJobStep;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;

import java.io.IOException;
import java.util.List;

public final class SummaryStep extends KMeansJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {
    JobStepConfig stepConfig = getConfig();

    String instanceDir = stepConfig.getInstanceDir();
    long generationID = stepConfig.getGenerationID();
    String outputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "summary/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    String inboundKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "inbound/";

    InboundSettings settings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    MRPipeline p = createBasicPipeline(StringSplitFn.class);
    PCollection<Record> records = toRecords(p.read(textInput(inboundKey)));
    PCollection<String> jsonSummary = getSummarizer(settings).buildJson(records);
    jsonSummary.write(compressedTextOutput(p.getConfiguration(), outputKey));
    return p;
  }

  public static Summarizer getSummarizer(InboundSettings settings) {
    return new Summarizer()
        .spec(getSpec(settings))
        .categoricalColumns(settings.getCategoricalColumns())
        .ignoreColumns(settings.getIgnoredColumns())
        .ignoreColumns(settings.getIdColumns());

  }

  private static Spec getSpec(InboundSettings settings) {
    List<String> columnNames = settings.getColumnNames();
    if (columnNames.isEmpty()) {
      return null;
    }
    RecordSpec.Builder rsb = RecordSpec.builder();
    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      if (settings.isNumeric(i)) {
        rsb.add(columnName, DataType.DOUBLE);
      } else {
        rsb.add(columnName, DataType.STRING);
      }
    }
    return rsb.build();
  }
}
