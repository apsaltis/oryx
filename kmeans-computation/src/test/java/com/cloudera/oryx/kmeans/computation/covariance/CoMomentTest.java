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

package com.cloudera.oryx.kmeans.computation.covariance;

import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.random.RandomManager;

public final class CoMomentTest extends OryxTest {

  @Test
  public void testMerge() throws Exception {
    CoMoment a = new CoMoment();
    CoMoment b = new CoMoment();
    CoMoment c = new CoMoment();
    RandomGenerator r = RandomManager.getRandom();
    for (int i = 0; i < 100; i++) {
      double r1 = r.nextDouble();
      double r2 = r.nextDouble();
      c.update(r1, r2);
      if (i % 2 == 0) {
        a.update(r1, r2);
      } else {
        b.update(r1, r2);
      }
    }
    assertEquals(c, a.merge(b));
    assertEquals(c, b.merge(a));
  }

  @Test
  public void testMergeZero() throws Exception {
    RandomGenerator r = RandomManager.getRandom();
    CoMoment cm = new CoMoment();
    for (int i = 0; i < 10; i++) {
      cm.update(r.nextDouble(), r.nextDouble());
    }
    assertEquals(cm, cm.merge(new CoMoment()));
    assertEquals(cm, new CoMoment().merge(cm));
  }
}
