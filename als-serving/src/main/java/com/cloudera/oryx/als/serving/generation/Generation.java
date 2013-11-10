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

package com.cloudera.oryx.als.serving.generation;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.math3.linear.RealMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.math.IllConditionedSolverException;
import com.cloudera.oryx.common.math.MatrixUtils;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.als.serving.candidate.CandidateFilter;
import com.cloudera.oryx.als.serving.candidate.CandidateFilterFactory;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Encapsulates a "generation" of output in the Oryx recommender implementation. This is essentially the output
 * from one run of the Computation Layer. It includes the generated model data -- at heart, the factored
 * {@code X} and {@code Y} matrices, but also things like the configured {@link CandidateFilter}, derived values,
 * and the set of known item IDs for each user.
 *
 * @author Sean Owen
 * @see GenerationLoader
 */
public final class Generation {

  private static final Logger log = LoggerFactory.getLogger(Generation.class);

  private final LongObjectMap<float[]> X;
  private Solver XTXsolver;
  private final LongObjectMap<float[]> Y;
  private Solver YTYsolver;
  private final StringLongMapping idMapping;
  private final LongObjectMap<LongSet> knownItemIDs;
  private CandidateFilter candidateFilter;
  private final ReadWriteLock xLock;
  private final ReadWriteLock yLock;
  private final ReadWriteLock knownItemLock;

  public Generation() {
    boolean noKnownItems = ConfigUtils.getDefaultConfig().getBoolean("model.no-known-items");
    this.X = new LongObjectMap<float[]>();
    this.XTXsolver = null;
    this.Y = new LongObjectMap<float[]>();
    this.YTYsolver = null;
    this.idMapping = new StringLongMapping();
    this.knownItemIDs = noKnownItems ? null : new LongObjectMap<LongSet>();
    this.candidateFilter = null;
    this.xLock = new ReentrantReadWriteLock();
    this.yLock = new ReentrantReadWriteLock();
    this.knownItemLock = new ReentrantReadWriteLock();
    recomputeState();
  }

  public void recomputeState() {
    XTXsolver = recomputeSolver(X, xLock.readLock());
    YTYsolver = recomputeSolver(Y, yLock.readLock());
    candidateFilter = new CandidateFilterFactory().buildCandidateFilter(Y, yLock.readLock());
  }

  private static Solver recomputeSolver(LongObjectMap<float[]> M, Lock readLock) {
    readLock.lock();
    try {
      if (M == null || M.isEmpty()) {
        return null;
      }
      RealMatrix MTM = MatrixUtils.transposeTimesSelf(M);
      double infNorm = MTM.getNorm();
      if (infNorm < 1.0) {
        log.warn("X'*X or Y'*Y has small inf norm ({}); try decreasing model.lambda", infNorm);
        throw new IllConditionedSolverException("infNorm: " + infNorm);
      }
      return MatrixUtils.getSolver(MTM);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * @return the number of "users" in the model (rows of {@link #getX()}
   */
  public int getNumUsers() {
    return X.size();
  }

  /**
   * @return the number of "items" in the model (rows of {@link #getY()}
   */
  public int getNumItems() {
    return Y.size();
  }

  /**
   * @return the user-feature matrix, implemented as a map from row number (user ID) to feature array
   */
  public LongObjectMap<float[]> getX() {
    return X;
  }

  /**
   * @return {@link Solver} for the matrix X' * X
   */
  public Solver getXTXSolver() {
    return XTXsolver;
  }

  /**
   * @return the item-feature matrix, implemented as a map from row number (item ID) to feature array
   */
  public LongObjectMap<float[]> getY() {
    return Y;
  }

  /**
   * @return {@link Solver} for the matrix Y' * Y
   */  
  public Solver getYTYSolver() {
    return YTYsolver;
  }

  public StringLongMapping getIDMapping() {
    return idMapping;
  }

  /**
   * @return the item IDs already associated to each user, as a map from user IDs to a set of item IDs
   */
  public LongObjectMap<LongSet> getKnownItemIDs() {
    return knownItemIDs;
  }

  public CandidateFilter getCandidateFilter() {
    return candidateFilter;
  }

  /**
   * Acquire this read/write lock before using {@link #getX()}.
   */
  public ReadWriteLock getXLock() {
    return xLock;
  }

  /**
   * Acquire this read/write lock before using {@link #getY()}.
   */
  public ReadWriteLock getYLock() {
    return yLock;
  }

  /**
   * Acquire this read/write lock before using {@link #getKnownItemIDs()}.
   */
  public ReadWriteLock getKnownItemLock() {
    return knownItemLock;
  }

}
