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

import com.google.common.primitives.Floats;
import org.junit.Test;

import java.io.File;

import com.cloudera.oryx.als.serving.ServerRecommender;

/**
 * Tests the ALS implementation using the simple
 * <a href="http://grouplens.org/datasets/movielens/">GroupLens</a> 100K data set. A new datum is
 * repeatedly added to the model to make sure that the approximate model update that results does not
 * yield invalid results.
 *
 * @author Sean Owen
 */
public final class RepeatedFoldInIT extends AbstractComputationIT {

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("grouplens100K");
  }

  @Test
  public void testFoldIn() throws Exception {
    ServerRecommender client = getRecommender();
    for (int i = 0; i < 10000; i++) {
      client.setPreference(Integer.toString(i % 5), Integer.toString(i % 3), 100.0f);
    }
    assertTrue(Floats.isFinite(client.estimatePreference("0", "0")));
    assertTrue(Floats.isFinite(client.estimatePreference("0", "1")));
    assertEquals(5, client.recommend("1", 5).size());
    client.refresh();
    assertTrue(Floats.isFinite(client.estimatePreference("0", "0")));
    assertTrue(Floats.isFinite(client.estimatePreference("0", "1")));
    assertEquals(5, client.recommend("1", 5).size());
  }

}
