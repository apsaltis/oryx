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

/**
 * Thrown when a method in classes like {@link OryxRecommender} can't complete because it is trying
 * to operate on a user whose ID is not known.
 *
 * @author Sean Owen
 */
public final class NoSuchUserException extends Exception {
  
  public NoSuchUserException() { }

  public NoSuchUserException(long userID) {
    this(String.valueOf(userID));
  }
  
  public NoSuchUserException(String message) {
    super(message);
  }
  
}
