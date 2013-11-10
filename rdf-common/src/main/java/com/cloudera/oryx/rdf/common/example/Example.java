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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * Encapsulates one example, or data point: a set of features that predict a target feature. Some features
 * may be missing, and in the case of test examples, the target value may be missing or unknown.
 *
 * @author Sean Owen
 */
public final class Example {

  private final Feature[] features;
  private final Feature target;
  private final int cachedHashCode;

  public Example(Feature target, Feature... features) {
    Preconditions.checkNotNull(features);
    this.features = features;
    this.target = target;
    cachedHashCode = Arrays.hashCode(features) ^ (target == null ? 0 : target.hashCode());
  }
  
  public Feature getFeature(int i) {
    return features[i];
  }

  public int getNumFeatures() {
    return features.length;
  }

  public Feature getTarget() {
    return target;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Example)) {
      return false;
    }
    Example other = (Example) o;
    return Arrays.equals(features, other.features) && Objects.equal(target, other.target);
  }

  @Override
  public int hashCode() {
    return cachedHashCode;
  }

  @Override
  public String toString() {
    return target == null ? Arrays.toString(features) : Arrays.toString(features) + " -> " + target;
  }
  
}
