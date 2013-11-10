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

package com.cloudera.oryx.als.computation;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.serving.ServerRecommender;

/**
 * Load tests the ALS implementation using the
 * <a href="http://grouplens.org/datasets/movielens/">GroupLens</a> 10M data set.
 *
 * @author Sean Owen
 */
public final class LoadIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(LoadIT.class);

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("grouplens10M-ABC");
  }

  @Test
  public void testLoad() throws Exception {
    ServerRecommender client = getRecommender();
    LoadRunner runner = new LoadRunner(client, TEST_TEMP_INBOUND_DIR, 20000);
    int cores = Runtime.getRuntime().availableProcessors();
    log.info("Load test with {} cores", cores);
    Stopwatch stopwatch = new Stopwatch().start();
    runner.runLoad();
    // assertTrue(stopwatch.elapsed(TimeUnit.MILLISECONDS) < (50 * runner.getSteps()) / cores);
    assertTrue(stopwatch.elapsedTime(TimeUnit.MILLISECONDS) < (50 * runner.getSteps()) / cores);
  }

}
