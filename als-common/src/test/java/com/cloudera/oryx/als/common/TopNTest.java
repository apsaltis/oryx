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

package com.cloudera.oryx.als.common;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link TopN}.
 *
 * @author Sean Owen
 */
public final class TopNTest extends OryxTest {

  @Test
  public void testEmpty() {
    Collection<NumericIDValue> top2 = TopN.selectTopN(Collections.<NumericIDValue>emptyList().iterator(), 2);
    assertNotNull(top2);
    assertEquals(0, top2.size());
  }

  @Test
  public void testTopExactly() {
    List<NumericIDValue> candidates = makeNCandidates(3);
    List<NumericIDValue> top3 = TopN.selectTopN(candidates.iterator(), 3);
    assertNotNull(top3);
    assertEquals(3, top3.size());
    assertEquals(3L, top3.get(0).getID());
    assertEquals(3.0f, top3.get(0).getValue());
    assertEquals(1L, top3.get(2).getID());
    assertEquals(1.0f, top3.get(2).getValue());
  }

  @Test
  public void testTopPlusOne() {
    List<NumericIDValue> candidates = makeNCandidates(4);
    List<NumericIDValue> top3 = TopN.selectTopN(candidates.iterator(), 3);
    assertNotNull(top3);
    assertEquals(3, top3.size());
    assertEquals(4L, top3.get(0).getID());
    assertEquals(4.0f, top3.get(0).getValue());
    assertEquals(2L, top3.get(2).getID());
    assertEquals(2.0f, top3.get(2).getValue());
  }

  @Test
  public void testTopOfMany() {
    List<NumericIDValue> candidates = makeNCandidates(20);
    List<NumericIDValue> top3 = TopN.selectTopN(candidates.iterator(), 3);
    assertNotNull(top3);
    assertEquals(3, top3.size());
    assertEquals(20L, top3.get(0).getID());
    assertEquals(20.0f, top3.get(0).getValue());
    assertEquals(18L, top3.get(2).getID());
    assertEquals(18.0f, top3.get(2).getValue());
  }

  @Test
  public void testTopOfManyIntoQueueMultithreaded() {
    BlockingQueue<NumericIDValue> top3 = Queues.newLinkedBlockingQueue();
    float[] queueLeastValue = { Float.NEGATIVE_INFINITY };
    List<NumericIDValue> candidates = makeNCandidates(20);
    TopN.selectTopNIntoQueueMultithreaded(top3, queueLeastValue, candidates.iterator(), 3);
    assertNotNull(top3);
    assertEquals(3, top3.size());
    assertEquals(18L, top3.peek().getID());
    assertEquals(18.0f, top3.peek().getValue());
  }


  private static List<NumericIDValue> makeNCandidates(int n) {
    List<NumericIDValue> candidates = Lists.newArrayListWithCapacity(n);
    for (int i = 1; i <= n; i++) {
      candidates.add(new NumericIDValue(i, i));
    }
    return candidates;
  }

}
