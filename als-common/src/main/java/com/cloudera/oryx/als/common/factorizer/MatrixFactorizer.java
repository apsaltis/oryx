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

package com.cloudera.oryx.als.common.factorizer;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.cloudera.oryx.common.collection.LongObjectMap;

/**
 * Implementations of this interface can factor a matrix into two matrices {@code X} and {@code Y},
 * typically of much lower rank.
 *
 * @author Sean Owen
 */
public interface MatrixFactorizer extends Callable<Object> {

  /**
   * Run the factorization algorithm to completion. Results are available from
   * {@link #getX()} and {@link #getY()} afterwards.
   *
   * @return {@code null}
   * @throws ExecutionException if an exception occurs during factorization;
   *  see {@link ExecutionException#getCause()} for reason
   * @throws InterruptedException if algorithm cannot complete because its computation
   *  was interrupted
   */
  @Override
  Void call() throws ExecutionException, InterruptedException;

  /**
   * Use the given matrix as the initial state of {@code Y}. May be ignored.
   *
   * @param previousY initial matrix state
   */
  void setPreviousY(LongObjectMap<float[]> previousY);

  /**
   * Typically called after {@link #call()} has finished.
   *
   * @return the current user-feature matrix, X
   */
  LongObjectMap<float[]> getX();

  /**
   * Typically called after {@link #call()} has finished.
   *
   * @return the current item-feature matrix, Y
   */
  LongObjectMap<float[]> getY();

}
