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

package com.cloudera.oryx.kmeans.computation.pmml;

import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.computation.common.summary.SummaryStats;
import com.cloudera.oryx.kmeans.common.Centers;
import com.cloudera.oryx.kmeans.computation.normalize.NormalizeSettings;
import com.cloudera.oryx.kmeans.computation.normalize.Transform;
import com.google.common.collect.Lists;
import org.apache.commons.math3.linear.RealVector;
import org.dmg.pmml.Apply;
import org.dmg.pmml.Array;
import org.dmg.pmml.Cluster;
import org.dmg.pmml.ClusteringField;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.ComparisonMeasure;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataType;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.Expression;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldRef;
import org.dmg.pmml.LinearNorm;
import org.dmg.pmml.LocalTransformations;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.NormContinuous;
import org.dmg.pmml.NormDiscrete;
import org.dmg.pmml.OpType;
import org.dmg.pmml.SquaredEuclidean;

import java.util.List;

public class ClusteringModelBuilder {

  private final MiningSchema miningSchema;
  private final DataDictionary dictionary;
  private LocalTransformations transforms;
  private List<ClusteringField> clusteringFields;

  public ClusteringModelBuilder(Summary summary) {
    InboundSettings settings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    this.miningSchema = PMMLUtils.buildMiningSchema(settings);
    this.dictionary = PMMLUtils.buildDataDictionary(settings, summary.getCategoryLevelsMapping());
    buildSummaryInfo(summary, settings);
  }

  public DataDictionary getDictionary() {
    return dictionary;
  }

  public ClusteringModel build(String modelName, Centers centers) {
    ClusteringModel model = new ClusteringModel(
        miningSchema,
        new ComparisonMeasure(ComparisonMeasure.Kind.DISTANCE).withMeasure(new SquaredEuclidean()),
        MiningFunctionType.CLUSTERING,
        ClusteringModel.ModelClass.CENTER_BASED,
        centers.size());
    model.setModelName(modelName);
    model.setAlgorithmName("K-Means||");
    model.setLocalTransformations(transforms);
    model.getClusteringFields().addAll(clusteringFields);
    for (int i = 0; i < centers.size(); i++) {
      model.getClusters().add(toCluster(centers.get(i), i));
    }
    return model;
  }

  private void buildSummaryInfo(Summary summary, InboundSettings settings) {
    this.transforms = new LocalTransformations();
    this.clusteringFields = Lists.newArrayList();
    NormalizeSettings normalize = NormalizeSettings.create(ConfigUtils.getDefaultConfig());
    List<String> columnNames = settings.getColumnNames();
    int offset = 0;
    for (int i = 0; i < columnNames.size(); i++) {
      if (settings.isIgnored(i)) {
        // Skip processing
      } else {
        SummaryStats ss = summary.getStats(i);
        FieldName baseName = new FieldName(columnNames.get(i));
        if (settings.isNumeric(i)) {
          FieldName fn = baseName;
          Transform t = normalize.getTransform(i);
          if (t != Transform.NONE) {
            Expression e = null;
            if (t == Transform.LINEAR) {
              e = new NormContinuous(baseName).withLinearNorms(
                  new LinearNorm(ss.min(), 0.0),
                  new LinearNorm(ss.max(), 1.0));
            } else if (t == Transform.LOG) {
              e = new Apply("ln").withExpressions(new FieldRef(baseName));
            } else if (t == Transform.Z) {
              e = new NormContinuous(baseName).withLinearNorms(
                  new LinearNorm(0.0, -ss.mean()/ss.stdDev()),
                  new LinearNorm(ss.mean(), 0.0));
            }
            fn = new FieldName(columnNames.get(i) + "_normed");
            DerivedField df = new DerivedField(OpType.CONTINUOUS, DataType.DOUBLE);
            df.setName(fn);
            df.setExpression(e);
            transforms.getDerivedFields().add(df);
            offset++;
          }
          ClusteringField cf = new ClusteringField(fn);
          if (normalize.getScale(i) != 1.0) {
            cf.setFieldWeight(normalize.getScale(i));
          }
          clusteringFields.add(cf);
        } else if (settings.isCategorical(i)) {
          List<String> levels = summary.getStats(i).getLevels();
          for (String level : levels) {
            DerivedField df = new DerivedField(OpType.CONTINUOUS, DataType.DOUBLE);
            FieldName fn = new FieldName(columnNames.get(i) + ':' + level);
            df.setName(fn);
            df.setExpression(new NormDiscrete(baseName, level));
            transforms.getDerivedFields().add(df);

            ClusteringField cf = new ClusteringField(fn);
            if (normalize.getScale(i) != 1.0) {
              cf.setFieldWeight(normalize.getScale(i));
            }
            clusteringFields.add(cf);
            offset++;
          }
        }
      }
    }
  }

  private static Cluster toCluster(RealVector point, int pointId) {
    Cluster cluster = new Cluster();
    cluster.setId(String.valueOf(pointId));
    Array array = new Array(toString(point), Array.Type.REAL);
    array.setN(point.getDimension());
    cluster.setArray(array);
    return cluster;
  }

  private static String toString(RealVector vec) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vec.getDimension(); i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(vec.getEntry(i));
    }
    return sb.toString();
  }
}
