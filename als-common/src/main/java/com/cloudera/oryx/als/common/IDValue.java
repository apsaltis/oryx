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

import java.io.Serializable;

/**
 * Simply encapsulates and ID and value.
 *
 * @author Sean Owen
 */
public final class IDValue implements Serializable {

  private final String id;
  private final float value;

  /**
   * Creates a new ID-value pair.
   *
   * @param id new ID
   * @param value new value
   */
  public IDValue(String id, float value) {
    this.id = id;
    this.value = value;
  }

  /**
   * @return ID
   */
  public String getID() {
    return id;
  }

  /**
   * @return value
   */
  public float getValue() {
    return value;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof IDValue)) {
      return false;
    }
    IDValue other = (IDValue) object;
    return
        ((id == null && other.getID() == null) || (id != null && id.equals(other.getID()))) &&
        getValue() == other.getValue();
  }

  @Override
  public int hashCode() {
    return (id == null ? 0 : id.hashCode()) ^ Float.floatToIntBits(value);
  }

  @Override
  public String toString() {
    return id + ':' + value;
  }

}
