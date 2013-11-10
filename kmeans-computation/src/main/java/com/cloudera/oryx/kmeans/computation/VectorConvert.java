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

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

import com.cloudera.oryx.common.math.AbstractRealVectorPreservingVisitor;
import com.cloudera.oryx.common.math.NamedRealVector;
import com.cloudera.oryx.common.math.OpenMapRealVector;
import com.cloudera.oryx.common.math.Vectors;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.avro.MLVector;
import com.cloudera.oryx.kmeans.computation.avro.MLWeightedVector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.RealVectorFormat;

/**
 * Utilities for converting from the Avro and the Java implementations of the common ML
 * type classes.
 */
public final class VectorConvert {

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.US);
  private static final RealVectorFormat VECTOR_FORMAT =
      new RealVectorFormat("", "", String.valueOf(DelimitedDataUtils.DELIMITER), NUMBER_FORMAT);

  static {
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  public static RealVector toVector(MLVector input) {
    RealVector base;
    if (input.getIndices().isEmpty()) {
      double[] d = new double[input.getSize()];
      for (int i = 0; i < d.length; i++) {
        d[i] = input.getValues().get(i);
      }
      base = Vectors.of(d);
    } else {
      List<Double> values = input.getValues();
      base = Vectors.sparse(input.getSize(), values.size());
      for (int i = 0; i < values.size(); i++) {
        base.setEntry(input.getIndices().get(i), values.get(i));
      }
    }
    if (input.getId().length() > 0) {
      base = new NamedRealVector(base, input.getId().toString());
    }
    return base;
  }
  
  public static MLVector fromVector(RealVector input) {
    final List<Double> values = Lists.newArrayList();
    MLVector.Builder vb = MLVector.newBuilder()
        .setSize(input.getDimension())
        .setValues(values);
    if (input instanceof ArrayRealVector) {
      vb.setIndices(ImmutableList.<Integer>of());
      for (int i = 0; i < input.getDimension(); i++) {
        values.add(input.getEntry(i));
      }
    } else {
      final List<Integer> indices = Lists.newArrayList();
      vb.setIndices(indices);
      input.walkInDefaultOrder(new AbstractRealVectorPreservingVisitor() {
        @Override
        public void visit(int index, double value) {
          indices.add(index);
          values.add(value);
        }
      });
    }
    if (input instanceof NamedRealVector) {
      vb.setId(((NamedRealVector) input).getName());
    } else {
      vb.setId("");
    }
    return vb.build();
  }

  public static WeightedRealVector toWeightedVector(MLWeightedVector weightedInput) {
    MLVector avroVec = weightedInput.getVec();
    RealVector realVec = toVector(avroVec);
    return new WeightedRealVector(realVec, weightedInput.getWeight());
  }

  public static MLWeightedVector fromWeightedVector(WeightedRealVector weightedInput) {
    RealVector realVec = weightedInput.thing();
    MLVector mlVec = fromVector(realVec);
    return new MLWeightedVector(mlVec, weightedInput.weight());
  }

  public static String toString(RealVector input) {
    return VECTOR_FORMAT.format(input);
  }

  public static RealVector fromString(CharSequence input) {
    RealVector rv = VECTOR_FORMAT.parse(input.toString());
    int density = 0;
    for (int i = 0; i < rv.getDimension(); i++) {
      if (rv.getEntry(i) != 0.0) {
        density++;
      }
    }
    if (2 * density < rv.getDimension()) {
      return new OpenMapRealVector(rv);
    } else {
      return rv;
    }
  }

  private VectorConvert() {}
}
