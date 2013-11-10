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

package com.cloudera.oryx.als.computation.initialy;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.fn.Aggregators;

public final class PreviousOrEmptyFeaturesAggregator extends Aggregators.SimpleAggregator<float[]> {

  private static final float[] EMPTY = {0.0f};

  private Iterable<float[]> values;

  @Override
  public void reset() {
    this.values = null;
  }

  @Override
  public void update(float[] value) {
    if (values == null && value != null && value.length > 0) {
      values = ImmutableList.of(value);
    }
  }

  @Override
  public Iterable<float[]> results() {
    return values == null ? ImmutableList.of(EMPTY) : values;
  }

}
