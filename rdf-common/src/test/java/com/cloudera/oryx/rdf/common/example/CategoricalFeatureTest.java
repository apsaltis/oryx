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

package com.cloudera.oryx.rdf.common.example;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link CategoricalFeature}.
 *
 * @author Sean Owen
 */
public final class CategoricalFeatureTest extends OryxTest {

  @Test
  public void testFeature() {
    CategoricalFeature f = CategoricalFeature.forValue(1);
    assertEquals(FeatureType.CATEGORICAL, f.getFeatureType());
    assertEquals(1, f.getValueID());
    assertEquals(f, CategoricalFeature.forValue(1));
    // Not necessary for correctness to assert this, but fairly important for performance
    assertSame(f, CategoricalFeature.forValue(1));
  }

}
