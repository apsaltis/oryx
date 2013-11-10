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

import com.cloudera.oryx.common.math.NamedRealVector;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.avro.MLVector;
import com.cloudera.oryx.kmeans.computation.avro.MLWeightedVector;

import org.apache.commons.math3.linear.RealVector;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

/**
 * Factory methods for creating {@code PType} instances for use with the ML Parallel libraries.
 */
public final class MLAvros {

  /**
   * Returns an {@code AvroType} based on the {@code AvroTypeFamily}.
   */
  public static AvroType<RealVector> vector() {
    return vector;
  }

  public static AvroType<NamedRealVector> namedVector() {
    return namedVector;
  }

  public static AvroType<WeightedRealVector> weightedVector() {
    return weightedVector;
  }

  private static final AvroType<RealVector> vector = Avros.derived(RealVector.class,
      new MapFn<MLVector, RealVector>() {
        @Override
        public RealVector map(MLVector vec) {
          return VectorConvert.toVector(vec);
        }
      },
      new MapFn<RealVector, MLVector>() {
        @Override
        public MLVector map(RealVector vec) {
          return VectorConvert.fromVector(vec);
        }
      },
      Avros.specifics(MLVector.class));


  private static final AvroType<NamedRealVector> namedVector = Avros.derived(NamedRealVector.class,
      new MapFn<MLVector, NamedRealVector>() {
        @Override
        public NamedRealVector map(MLVector vec) {
          return (NamedRealVector) VectorConvert.toVector(vec);
        }
      },
      new MapFn<NamedRealVector, MLVector>() {
        @Override
        public MLVector map(NamedRealVector vec) {
          return VectorConvert.fromVector(vec);
        }
      },
      Avros.specifics(MLVector.class));

  private static final AvroType<WeightedRealVector> weightedVector = Avros.derived(WeightedRealVector.class,
      new MapFn<MLWeightedVector, WeightedRealVector>() {
        @Override
        public WeightedRealVector map(MLWeightedVector vec) {
          return VectorConvert.toWeightedVector(vec);
        }
      },
      new MapFn<WeightedRealVector, MLWeightedVector>() {
        @Override
        public MLWeightedVector map(WeightedRealVector vec) {
          return VectorConvert.fromWeightedVector(vec);
        }
      },
      Avros.specifics(MLWeightedVector.class)
  );

  static {
    Avros.register(RealVector.class, vector);
    Avros.register(NamedRealVector.class, namedVector);
  }

  private MLAvros() {}
}
