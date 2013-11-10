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

package com.cloudera.oryx.computation.common;

import com.google.common.base.Preconditions;

/**
 * Encapsulates a dependency between two things, such that one must happen after the other, and depends on 
 * the first occurring first.
 * 
 * @author Sean Owen
 * @param <T> the type of thing for which dependencies are expressed
 */
public final class DependsOn<T> {
  
  private final T happensNext;  
  private final T happensFirst;

  /**
   * @param happensNext thing that should happen after {@code happensFirst}
   * @param happensFirst thing that should happen before {@code happensNext}
   */
  public DependsOn(T happensNext, T happensFirst) {
    Preconditions.checkNotNull(happensNext);    
    Preconditions.checkNotNull(happensFirst);
    this.happensNext = happensNext;
    this.happensFirst = happensFirst;    
  }

  /**
   * @param happensFirst thing that should happen, with no prerequisite
   */
  public DependsOn(T happensFirst) {
    Preconditions.checkNotNull(happensFirst);
    this.happensNext = null;
    this.happensFirst = happensFirst;    
  }

  /**
   * Convenience factory method for {@link #DependsOn(Object, Object)}
   */
  public static <T> DependsOn<T> nextAfterFirst(T happensNext, T happenstFirst) {
    return new DependsOn<T>(happensNext, happenstFirst);
  }

  /**
   * Convenience factory method for {@link #DependsOn(Object)}
   */
  public static <T> DependsOn<T> first(T happensFirst) {
    return new DependsOn<T>(happensFirst);
  }

  /**
   * @return thing that has to happen next. May be {@code null} if there is no prerequisite expressed.
   */
  T getHappensNext() {
    return happensNext;
  }
  
  /**
   * @return thing that has to happen first.
   */
  T getHappensFirst() {
    return happensFirst;
  }  
  
  @Override
  public String toString() {
    if (happensNext == null) {
      return "(" + happensFirst + ')';
    } else {
      return "(" + happensNext + " -> " + happensFirst + ')';      
    }
  }

}
