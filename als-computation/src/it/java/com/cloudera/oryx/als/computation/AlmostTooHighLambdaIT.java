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

import org.junit.Test;

import com.cloudera.oryx.als.serving.ServerRecommender;

/**
 * Tests case where {@code als-model.lambda} is almost too high but should still allow a valid model.
 * 
 * @author Sean Owen
 */
public final class AlmostTooHighLambdaIT extends AbstractComputationIT {

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("highlambda");
  }

  @Test
  public void testWaitForBuild() throws Exception {
    ServerRecommender client = getRecommender();
    client.ingest(new File(TEST_TEMP_INBOUND_DIR, "lambda.csv.gz"));
  }

}
