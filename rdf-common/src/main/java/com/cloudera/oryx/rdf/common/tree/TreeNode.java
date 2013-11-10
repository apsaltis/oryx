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

package com.cloudera.oryx.rdf.common.tree;

/**
 * Implementations of this interface represent nodes in a {@link DecisionTree}, whether leaves ({@link TerminalNode})
 * or internal nodes ({@link DecisionNode}).
 *
 * @author Sean Owen
 * @see TerminalNode
 * @see DecisionNode
 */
public interface TreeNode {

  /**
   * @return true iff the node is a leaf ({@link TerminalNode}) rather than a {@link DecisionNode}
   */
  boolean isTerminal();

  boolean equals(Object o);

  int hashCode();
  
}
