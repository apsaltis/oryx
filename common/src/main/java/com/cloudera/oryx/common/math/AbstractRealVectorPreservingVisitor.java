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

package com.cloudera.oryx.common.math;

import org.apache.commons.math3.linear.RealVectorPreservingVisitor;

/**
 * Dummy implementation of {@link RealVectorPreservingVisitor} which implements its methods with no-ops.
 *
 * @author Sean Owen
 */
public abstract class AbstractRealVectorPreservingVisitor implements RealVectorPreservingVisitor {

  @Override
  public void start(int dimension, int start, int end) {
    // do nothing
  }

  @Override
  public double end() {
    // do nothing
    return Double.NaN;
  }

}
