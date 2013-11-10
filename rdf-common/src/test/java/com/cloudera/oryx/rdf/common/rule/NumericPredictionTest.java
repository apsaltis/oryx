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

package com.cloudera.oryx.rdf.common.rule;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.example.NumericFeature;

/**
 * Tests {@link NumericPrediction}.
 *
 * @author Sean Owen
 */
public final class NumericPredictionTest extends OryxTest {

  @Test
  public void testConstruct() {
    NumericPrediction prediction = new NumericPrediction(1.5f, 1);
    assertEquals(FeatureType.NUMERIC, prediction.getFeatureType());
    assertEquals(1.5f, prediction.getPrediction());
  }

  @Test
  public void testUpdate() {
    NumericPrediction prediction = new NumericPrediction(1.5f, 1);
    Example example = new Example(NumericFeature.forValue(2.5f));
    prediction.update(example);
    assertEquals(2.0f, prediction.getPrediction());
  }

}
