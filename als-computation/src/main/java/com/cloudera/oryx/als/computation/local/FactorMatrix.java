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

package com.cloudera.oryx.als.computation.local;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.cloudera.oryx.als.common.factorizer.als.AlternatingLeastSquares;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.math.SingularMatrixSolverException;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobException;

final class FactorMatrix implements Callable<AlternatingLeastSquares> {

  private static final Logger log = LoggerFactory.getLogger(FactorMatrix.class);

  private final LongObjectMap<LongFloatMap> RbyRow;
  private final LongObjectMap<LongFloatMap> RbyColumn;

  FactorMatrix(LongObjectMap<LongFloatMap> rbyRow, LongObjectMap<LongFloatMap> rbyColumn) {
    RbyRow = rbyRow;
    RbyColumn = rbyColumn;
  }

  @Override
  public AlternatingLeastSquares call() throws InterruptedException, JobException {
    try {
      log.info("Building factorization...");

      Config config = ConfigUtils.getDefaultConfig();
      int features = config.getInt("model.features");
      double convergenceThreshold = config.getDouble("model.iterations.convergence-threshold");
      int maxIterations = config.getInt("model.iterations.max");
      AlternatingLeastSquares als = new AlternatingLeastSquares(RbyRow,
                                                                RbyColumn,
                                                                features,
                                                                convergenceThreshold,
                                                                maxIterations);

      try {
        als.call();
        log.info("Factorization complete");
      } catch (ExecutionException e) {
        throw new JobException(e.getCause());
      }

      return als;
    } catch (SingularMatrixSolverException smse) {
      int features = ConfigUtils.getDefaultConfig().getInt("model.features");
      int fewerFeatures = smse.getApparentRank();
      log.warn("Could not build model with {} features; try model.features={}",
               features, fewerFeatures);
      throw smse;
    }
  }

}
