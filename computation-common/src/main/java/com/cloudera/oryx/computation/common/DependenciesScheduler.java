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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for sorting out an order of execution for a series of tasks where some must
 * precede others.
 *
 * @author Sean Owen
 * @param <T> type of things to schedule
 */
final class DependenciesScheduler<T> {

  private static final Logger log = LoggerFactory.getLogger(DependenciesScheduler.class);

  /**
   * @param dependencies dependencies, expressed as {@link DependsOn} objects.
   * @return a series of groups of objects, wherein the members of each group may only run after the members
   *  of all earlier groups are done.
   */
  List<Collection<T>> schedule(Collection<DependsOn<T>> dependencies) {

    log.info("Scheduling: {}", dependencies);

    // This will map steps to a collection of all steps that must come before
    Map<T,Collection<T>> prerequisites = Maps.newHashMapWithExpectedSize(dependencies.size());

    for (DependsOn<T> dependency : dependencies) {

      T first = dependency.getHappensFirst();      
      T next = dependency.getHappensNext();

      // Just make sure it's noted
      if (!prerequisites.containsKey(first)) {
        prerequisites.put(first, Lists.<T>newArrayList());
      }

      if (next != null) {
        Collection<T> required = prerequisites.get(next);
        if (required == null) {
          required = Lists.newArrayList();
          prerequisites.put(next, required);
        }
        required.add(first);
      }
    }

    return schedule(prerequisites);
  }
  
  private List<Collection<T>> schedule(Map<T,Collection<T>> prerequisites) {

    // This will be a list of collections of steps; each collection can be executed in parallel
    // but everything in each collection must finish before the next collection
    List<Collection<T>> steps = Lists.newArrayList();
    Map<T,Collection<T>> clone = Maps.newHashMap(prerequisites);
    
    while (!clone.isEmpty()) {
      
      Collection<T> currentStep = Lists.newArrayList();
      
      Iterator<Map.Entry<T,Collection<T>>> it = clone.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<T,Collection<T>> entry = it.next();
        Collection<T> value = entry.getValue();
        if (value == null || value.isEmpty()) {
          currentStep.add(entry.getKey());
          it.remove();
        }
      }

      Preconditions.checkState(!currentStep.isEmpty(), "Circular dependency?");

      for (Collection<T> value : clone.values()) {
        if (value != null) {
          value.removeAll(currentStep);
        }
      }
      
      steps.add(currentStep);
      
    }

    log.info("Schedule is {}", steps);

    return steps;
  }

}
