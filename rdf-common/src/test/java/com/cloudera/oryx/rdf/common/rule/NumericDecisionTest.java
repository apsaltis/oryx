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
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.example.NumericFeature;

/**
 * Tests {@link NumericDecision}.
 *
 * @author Sean Owen
 */
public final class NumericDecisionTest extends OryxTest {

  @Test
  public void testDecisionBasics() {
    NumericDecision decision = new NumericDecision(0, -1.5f, false);
    assertEquals(0, decision.getFeatureNumber());
    assertEquals(-1.5f, decision.getThreshold());
    assertFalse(decision.getDefaultDecision());
    assertEquals(FeatureType.NUMERIC, decision.getType());
  }

  @Test
  public void testDecision() {
    Decision decision = new NumericDecision(0, -3.1f, true);
    assertFalse(decision.isPositive(new Example(null, NumericFeature.forValue(-3.5f))));
    assertTrue(decision.isPositive(new Example(null, NumericFeature.forValue(-3.1f))));
    assertTrue(decision.isPositive(new Example(null, NumericFeature.forValue(-3.0f))));
    assertTrue(decision.isPositive(new Example(null, NumericFeature.forValue(3.1f))));
    assertTrue(decision.isPositive(new Example(null, new Feature[] {null})));
  }

}
