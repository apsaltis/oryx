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

package com.cloudera.oryx.computation.common.summary;

import java.io.Serializable;

public final class Entry implements Serializable {

  private long count;
  
  public Entry() {
    this(0L);
  }
  
  public Entry(long count) {
    this.count = count;
  }
  
  public long getCount() {
    return count;
  }
  
  public Entry inc() {
    return inc(1L);
  }
  
  public Entry inc(long count) {
    this.count += count;
    return this;
  } 
}