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

import com.cloudera.oryx.common.math.Vectors;
import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.commons.math3.linear.RealVector;
import org.dmg.pmml.Apply;
import org.dmg.pmml.ClusteringField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.Expression;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldRef;
import org.dmg.pmml.LinearNorm;
import org.dmg.pmml.LocalTransformations;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.NormContinuous;
import org.dmg.pmml.NormDiscrete;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class VectorFactory {
  private final boolean sparse;
  private final int fieldCount;
  private final List<Set<Update>> updates;

  public static VectorFactory create(
      MiningSchema schema,
      LocalTransformations transforms,
      List<ClusteringField> fields) {
    Map<FieldName, DerivedField> derived = Maps.newHashMapWithExpectedSize(transforms.getDerivedFields().size());
    for (DerivedField df : transforms.getDerivedFields()) {
      derived.put(df.getName(), df);
    }

    Multimap<FieldName, NumericUpdate> numeric = HashMultimap.create();
    Map<FieldName, Map<String, Update>> categorical = Maps.newHashMap();
    for (int j = 0; j < fields.size(); j++) {
      ClusteringField cf = fields.get(j);
      FieldName fn = cf.getField();
      if (derived.containsKey(fn)) {
        DerivedField df = derived.get(fn);
        Expression e = df.getExpression();
        if (e instanceof NormDiscrete) {
          NormDiscrete nd = (NormDiscrete) e;
          Map<String, Update> m = categorical.get(nd.getField());
          if (m == null) {
            m = Maps.newHashMap();
            categorical.put(nd.getField(), m);
          }
          m.put(nd.getValue(), new NumericUpdate(ONE, j, cf.getFieldWeight()));
        } else if (e instanceof Apply) {
          Apply apply = (Apply) e;
          if (!"ln".equals(apply.getFunction())) {
            throw new UnsupportedOperationException("Unsupported function type: " + apply.getFunction());
          }
          FieldName f = ((FieldRef) apply.getExpressions().get(0)).getField();
          numeric.put(f, new NumericUpdate(LOG_VALUE, j, cf.getFieldWeight()));
        } else if (e instanceof NormContinuous) {
          NormContinuous nc = (NormContinuous) e;
          FieldName f = nc.getField();
          LinearNorm l1 = nc.getLinearNorms().get(0);
          LinearNorm l2 = nc.getLinearNorms().get(1);
          InterpolateFunction ifunc = new InterpolateFunction(
              l1.getOrig(), l1.getNorm(),
              l2.getOrig(), l2.getNorm());
          numeric.put(f, new NumericUpdate(ifunc, j, cf.getFieldWeight()));
        } else {
          throw new UnsupportedOperationException("Unsupported expression type: " + e);
        }
      } else {
        numeric.put(fn, new NumericUpdate(VALUE, j, cf.getFieldWeight()));
      }
    }

    boolean sparse = 2 * schema.getMiningFields().size() <= fields.size();
    List<Set<Update>> updates = Lists.newArrayListWithExpectedSize(schema.getMiningFields().size());
    for (MiningField mf : schema.getMiningFields()) {
      FieldName fn = mf.getName();
      if (numeric.containsKey(fn)) {
        updates.add(ImmutableSet.<Update>copyOf(numeric.get(fn)));
      } else if (categorical.containsKey(fn)) {
        CategoricalUpdate u = new CategoricalUpdate(categorical.get(fn));
        updates.add(ImmutableSet.<Update>of(u));
      }
    }
    return new VectorFactory(sparse, fields.size(), updates);
  }

  private VectorFactory(boolean sparse, int fieldCount, List<Set<Update>> updates) {
    this.sparse = sparse;
    this.fieldCount = fieldCount;
    this.updates = updates;
  }

  public RealVector createVector(String[] tokens) {
    if (tokens.length != updates.size()) {
      return null;
    }
    RealVector v = sparse ? Vectors.sparse(fieldCount) : Vectors.dense(fieldCount);
    for (int i = 0; i < tokens.length; i++) {
      for (Update update : updates.get(i)) {
        update.update(v, tokens[i]);
      }
    }
    return v;
  }

  private interface Update {
    void update(RealVector v, String value);
  }

  private static class CategoricalUpdate implements Update {
    private Map<String, Update> updates;

    CategoricalUpdate(Map<String, Update> updates) {
      this.updates = updates;
    }

    @Override
    public void update(RealVector v, String value) {
      updates.get(value).update(v, value);
    }
  }

  private static class NumericUpdate implements Update {
    private ConvertFunction c;
    private int offset;
    private double scale;

    NumericUpdate(ConvertFunction c, int offset, double scale) {
      this.c = c;
      this.offset = offset;
      this.scale = scale;
    }

    @Override
    public void update(RealVector v, String value) {
      v.setEntry(offset, c.apply(value) * scale);
    }
  }

  private interface ConvertFunction extends Function<String, Double> {}

  private static final ConvertFunction ONE = new ConvertFunction() {
    @Override
    public Double apply(String input) {
      return 1.0;
    }
  };

  private static final ConvertFunction VALUE = new ConvertFunction() {
    @Override
    public Double apply(String input) {
      try {
        return Double.valueOf(input);
      } catch (NumberFormatException e) {
        return Double.NaN;
      }
    }
  };
  private static final ConvertFunction LOG_VALUE = new ConvertFunction() {
    @Override
    public Double apply(String input) {
      try {
        return Math.log(Double.valueOf(input));
      } catch (NumberFormatException e) {
        return Double.NaN;
      }
    }
  };

  private static class InterpolateFunction implements ConvertFunction {
    private final double a1;
    private final double b1;
    private final double a2;
    private final double b2;

    private InterpolateFunction(double a1, double b1, double a2, double b2) {
      this.a1 = a1;
      this.b1 = b1;
      this.a2 = a2;
      this.b2 = b2;
    }

    @Override
    public Double apply(String input) {
      try {
        double x = Double.valueOf(input);
        return b1 + ((x - a1) / (a2 - a1)) * (b2 - b1);
      } catch (NumberFormatException e) {
        return Double.NaN;
      }
    }
  }
}
