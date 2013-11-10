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

import com.cloudera.oryx.kmeans.common.FixedKEvalStrategy;
import com.cloudera.oryx.kmeans.common.KMeansEvalStrategy;
import com.cloudera.oryx.kmeans.common.LowCostStableEvalStrategy;
import com.typesafe.config.Config;

import com.cloudera.oryx.kmeans.common.KMeansInitStrategy;
import com.cloudera.oryx.kmeans.common.KMeansUpdateStrategy;
import com.cloudera.oryx.kmeans.common.LloydsUpdateStrategy;
import com.cloudera.oryx.kmeans.common.MiniBatchUpdateStrategy;

import java.io.Serializable;
import java.util.List;

public final class EvaluationSettings implements Serializable {

  private final List<Integer> kValues;
  private final int replications;
  private final int folds;
  private final int parallelism;
  private final KMeansInitStrategy initStrategy;
  private final KMeansUpdateStrategy updateStrategy;
  private final KMeansEvalStrategy evalStrategy;

  public static EvaluationSettings create(Config config) {
    Config kmeans = config.getConfig("model");
    KMeansInitStrategy initStrategy = KMeansInitStrategy.valueOf(kmeans.getString("init-strategy"));

    Config update = kmeans.getConfig("update-strategy");
    String updateStrategyName = update.getString("type");
    KMeansUpdateStrategy updateStrategy;
    if ("LLOYDS".equalsIgnoreCase(updateStrategyName)) {
      updateStrategy = new LloydsUpdateStrategy(update.getInt("iterations"));
    } else if ("MINIBATCH".equalsIgnoreCase(updateStrategyName)) {
      updateStrategy = new MiniBatchUpdateStrategy(update.getInt("iterations"), update.getInt("batch-size"), null);
    } else {
      throw new IllegalArgumentException(String.format(
          "Unknown update strategy: '%s' (valid options are LLOYDS and MINIBATCH)",
          updateStrategyName));
    }

    KMeansEvalStrategy evalStrategy = null;
    if (kmeans.hasPath("eval-strategy")) {
      Config eval = kmeans.getConfig("eval-strategy");
      String evalStrategyName = eval.getString("type");
      if ("FIXED".equalsIgnoreCase(evalStrategyName)) {
        evalStrategy = new FixedKEvalStrategy(config.getInt("k"));
      } else if ("THRESHOLD".equalsIgnoreCase(evalStrategyName)) {
        boolean varOfInfo = !eval.hasPath("criterion") || "vi".equalsIgnoreCase(config.getString("criterion"));
        evalStrategy = new LowCostStableEvalStrategy(eval.getDouble("threshold"), varOfInfo);
      } else {
        throw new IllegalArgumentException(String.format(
            "Unknown eval strategy: '%s' (valid options are FIXED and THRESHOLD)",
            evalStrategyName));
      }
    }
    return new EvaluationSettings(
        kmeans.getIntList("k"),
        kmeans.getInt("replications"),
        kmeans.getInt("cross-folds"),
        kmeans.getInt("parallelism"),
        initStrategy,
        updateStrategy,
        evalStrategy);
  }

  public EvaluationSettings(List<Integer> kValues, int replications, int folds, int parallelism,
                            KMeansInitStrategy initStrategy, KMeansUpdateStrategy updateStrategy,
                            KMeansEvalStrategy evalStrategy) {
    this.kValues = kValues;
    this.replications = replications;
    this.folds = folds;
    this.parallelism = parallelism;
    this.initStrategy = initStrategy;
    this.updateStrategy = updateStrategy;
    this.evalStrategy = evalStrategy;
  }

  public List<Integer> getKValues() {
    return kValues;
  }

  public int getReplications() {
    return replications;
  }

  public int getFolds() {
    return folds;
  }

  public int getParallelism() {
    return parallelism;
  }

  public KMeansInitStrategy getInitStrategy() {
    return initStrategy;
  }

  public KMeansUpdateStrategy getUpdateStrategy() {
    return updateStrategy;
  }

  public KMeansEvalStrategy getEvalStrategy() {
    return evalStrategy;
  }
}
