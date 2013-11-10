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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.oryx.als.common;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Utility methods for finding the top N things from a stream.
 *
 * @author Sean Owen
 * @author Mahout
 */
public final class TopN {

  private TopN() {
  }

  /**
   * @param n how many top values to choose
   * @return Initialized {@link Queue} suitable for use in this class
   */
  public static Queue<NumericIDValue> initialQueue(int n) {
    return new PriorityQueue<NumericIDValue>(n + 2, ByValueAscComparator.INSTANCE);
  }

  /**
   * Computes top N values for a stream and puts them into a {@link Queue}.
   *
   * @param topN {@link Queue} to add to
   * @param values stream of values from which to choose
   * @param n how many top values to choose
   */
  public static void selectTopNIntoQueue(Queue<NumericIDValue> topN,
                                         Iterator<NumericIDValue> values,
                                         int n) {
    while (values.hasNext()) {
      NumericIDValue value = values.next();
      if (value != null) {
        long itemID = value.getID();
        float valueScore = value.getValue();
        if (topN.size() >= n) {
          if (valueScore > topN.peek().getValue()) {
            NumericIDValue recycled = topN.poll();
            recycled.set(itemID, valueScore);
            topN.add(recycled);
          }
        } else {
          topN.add(new NumericIDValue(itemID, valueScore));
        }
      }
    }
  }


  /**
   * Computes top N values for a stream and puts them into a {@link Queue}.
   * Used in the context of multiple threads.
   *
   * @param topN {@link Queue} to add to
   * @param queueLeastValue in/out parameter caching the queue's least value
   * @param values stream of values from which to choose
   * @param n how many top values to choose
   */
  public static void selectTopNIntoQueueMultithreaded(Queue<NumericIDValue> topN,
                                                      float[] queueLeastValue,
                                                      Iterator<NumericIDValue> values,
                                                      int n) {
    // Cache to avoid most synchronization
    float localQueueLeastValue = queueLeastValue[0];

    while (values.hasNext()) {
      NumericIDValue value = values.next();
      if (value != null) {
        long itemID = value.getID();
        float valueScore = value.getValue();
        if (valueScore >= localQueueLeastValue) {

          synchronized (topN) {
            if (topN.size() >= n) {
              float currentQueueLeastValue = topN.peek().getValue();
              localQueueLeastValue = currentQueueLeastValue;
              if (valueScore > currentQueueLeastValue) {
                NumericIDValue recycled = topN.poll();
                recycled.set(itemID, valueScore);
                topN.add(recycled);
              }
            } else {
              topN.add(new NumericIDValue(itemID, valueScore));
            }
          }
        }
      }
    }

    queueLeastValue[0] = localQueueLeastValue;
  }

  /**
   * @param topN {@link Queue} of items from which to take top n
   * @param n how many top values to choose
   * @return order list of top results
   */
  public static List<NumericIDValue> selectTopNFromQueue(Queue<NumericIDValue> topN, int n) {
    if (topN.isEmpty()) {
      return Collections.emptyList();
    }
    while (topN.size() > n) {
      NumericIDValue removed = topN.poll();
      Preconditions.checkNotNull(removed);
    }
    List<NumericIDValue> result = Lists.newArrayList(topN);
    Collections.sort(result, Collections.reverseOrder(ByValueAscComparator.INSTANCE));
    return result;
  }

  /**
   * @param values stream of values from which to choose
   * @param n how many top values to choose
   * @return the top N values (at most), ordered by value descending.
   */
  public static List<NumericIDValue> selectTopN(Iterator<NumericIDValue> values, int n) {
    Queue<NumericIDValue> topN = initialQueue(n);
    selectTopNIntoQueue(topN, values, n);
    return selectTopNFromQueue(topN, n);
  }

}
