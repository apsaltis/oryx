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

package com.cloudera.oryx.als.computation.iterate.row;

import com.cloudera.oryx.als.computation.types.MatrixRow;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.math.MatrixUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.fn.OryxReduceMapFn;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.apache.crunch.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RowReduceFn extends OryxReduceMapFn<Long, Iterable<LongFloatMap>, MatrixRow> {

  private static final Logger log = LoggerFactory.getLogger(RowReduceFn.class);

  private final YState yState;
  private double alpha;
  private double lambda;
  private boolean reconstructRMatrix;
  private boolean lossIgnoresUnspecified;

  public RowReduceFn(YState yState) {
    this.yState = yState;
  }

  @Override
  public void initialize() {
    super.initialize();

    Config config = ConfigUtils.getDefaultConfig();
    alpha = config.getDouble("model.alpha");
    lambda = alpha * config.getDouble("model.lambda");

    // This will cause the ALS algorithm to reconstruction the input matrix R, rather than the
    // matrix P = R > 0 . Don't use this unless you understand it!
    reconstructRMatrix = config.getBoolean("model.reconstruct-r-matrix");
    // Causes the loss function to exclude entries for any input pairs that do not appear in the
    // input and are implicitly 0
    // Likewise, don't touch this for now unless you know what it does.
    lossIgnoresUnspecified = config.getBoolean("model.loss-ignores-unspecified");

    log.info("alpha = {}, lambda = {}", alpha, lambda);

    yState.initialize(getContext(), getPartition(), getNumPartitions());
  }

  @Override
  public MatrixRow map(Pair<Long, Iterable<LongFloatMap>> input) {
    LongFloatMap values = Iterables.getOnlyElement(input.second());

    LongObjectMap<float[]> Y = yState.getY();
    RealMatrix YTY = yState.getYTY();

    // Start computing Wu = (YT*Cu*Y + lambda*I) = (YT*Y + YT*(Cu-I)*Y + lambda*I),
    // by first starting with a copy of YT * Y. Or, a variant on YT * Y, if LOSS_IGNORES_UNSPECIFIED is set
    RealMatrix Wu;
    if (lossIgnoresUnspecified) {
      Wu = partialTransposeTimesSelf(Y, YTY.getRowDimension(), values.entrySet());
    } else {
      Wu = YTY.copy();
    }

    double[][] WuData = MatrixUtils.accessMatrixDataDirectly(Wu);
    int features = Wu.getRowDimension();
    double[] YTCupu = new double[features];

    for (LongFloatMap.MapEntry entry : values.entrySet()) {

      double xu = entry.getValue();
      long itemID = entry.getKey();
      float[] vector = Y.get(itemID);
      Preconditions.checkNotNull(vector, "No feature vector for %s", itemID);

      // Wu and YTCupu
      if (reconstructRMatrix) {
        for (int row = 0; row < features; row++) {
          YTCupu[row] += xu * vector[row];
        }
      } else {
        double cu = 1.0 + alpha * FastMath.abs(xu);
        for (int row = 0; row < features; row++) {
          float vectorAtRow = vector[row];
          double rowValue = vectorAtRow * (cu - 1.0);
          double[] WuDataRow = WuData[row];
          for (int col = 0; col < features; col++) {
            WuDataRow[col] += rowValue * vector[col];
            //Wu.addToEntry(row, col, rowValue * vector[col]);
          }
          if (xu > 0.0) {
            YTCupu[row] += vectorAtRow * cu;
          }
        }
      }

    }

    Preconditions.checkState(!values.isEmpty(), "No values for user {}?", input.first());

    double lambdaTimesCount = lambda * values.size();
    for (int x = 0; x < features; x++) {
      WuData[x][x] += lambdaTimesCount;
      //Wu.addToEntry(x, x, lambdaTimesCount);
    }

    float[] xu = MatrixUtils.getSolver(Wu).solveDToF(YTCupu);
    return new MatrixRow(input.first(), xu);
  }

  private static RealMatrix partialTransposeTimesSelf(LongObjectMap<float[]> M,
                                                      int dimension,
                                                      Iterable<LongFloatMap.MapEntry> entries) {
    RealMatrix result = new Array2DRowRealMatrix(dimension, dimension);
    for (LongFloatMap.MapEntry entry : entries) {
      float[] vector = M.get(entry.getKey());
      for (int row = 0; row < dimension; row++) {
        float rowValue = vector[row];
        for (int col = 0; col < dimension; col++) {
          result.addToEntry(row, col, rowValue * vector[col]);
        }
      }
    }
    return result;
  }
}
