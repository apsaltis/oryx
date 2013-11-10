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

import com.google.common.primitives.Longs;

import java.io.Serializable;

/**
 * Simply encapsulates and ID and value.
 *
 * @author Sean Owen
 */
public final class NumericIDValue implements Serializable {

  private long id;
  private float value;

  public long getID() {
    return id;
  }

  public float getValue() {
    return value;
  }

  public NumericIDValue() {
    this.id = Long.MIN_VALUE;
    this.value = Float.NaN;
  }

  public NumericIDValue(long id, float value) {
    this.id = id;
    this.value = value;
  }

  public void set(long itemID, float value) {
    this.id = itemID;
    this.value = value;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof NumericIDValue)) {
      return false;
    }
    NumericIDValue other = (NumericIDValue) object;
    return getID() == other.getID() && getValue() == other.getValue();
  }

  @Override
  public int hashCode() {
    return Longs.hashCode(id) ^ Float.floatToIntBits(value);
  }

  @Override
  public String toString() {
    return id + ":" + value;
  }

}
