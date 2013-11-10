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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.ClassUtils;
import com.cloudera.oryx.common.servcomp.OryxConfiguration;

/**
 * A step which actually runs several {@link JobStep}s in parallel.
 *
 * @author Sean Owen
 */
public final class ParallelStep extends Configured implements Tool, HasState {

  private static final Logger log = LoggerFactory.getLogger(ParallelStep.class);

  private final Collection<JobStep> steps;

  public ParallelStep() {
    // steps = Lists.newCopyOnWriteArrayList();
    steps = new CopyOnWriteArrayList<JobStep>();
  }

  /**
   * @return result of {@code getStepStates()} from this step's parallel component steps.
   */
  @Override
  public Collection<StepState> getStepStates() throws IOException, InterruptedException {
    Collection<StepState> states = Lists.newArrayList();
    for (JobStep step : steps) {
      states.addAll(step.getStepStates());
    }
    return states;
  }

  /**
   * @param args will be passed through to the parallel steps, except for the last. The last is a
   * comma-separated list of class names of steps to execute in parallel.
   */
  @Override
  public int run(final String[] args) throws JobException, IOException, InterruptedException {

    // all but last arg are passed through
    String[] stepClassNames = args[args.length - 1].split(",");

    Collection<Callable<Object>> runnables = Lists.newArrayList();

    for (String stepClassName : stepClassNames) {
      final JobStep step = ClassUtils.loadInstanceOf(stepClassName, JobStep.class);
      steps.add(step);
      runnables.add(new Callable<Object>() {
        @Override
        public Void call() throws Exception {
          log.info("Running step {}", step.getClass().getSimpleName());
          ToolRunner.run(getConf(), step, args);
          return null;
        }
      });
    }

    ExecutorService executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("ParallelStep-%d").build());
    Iterable<Future<Object>> futures;
    try {
      futures = executor.invokeAll(runnables);
    } finally {
      // shutdown() now rather than shutdownNow() later; this may let some parallel jobs finish
      executor.shutdown();
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof InterruptedException) {
          log.warn("Interrupted");
          throw (InterruptedException) cause;
        }
        if (cause instanceof IOException) {
          log.warn("Unexpected exception while running step", cause);
          throw (IOException) cause;
        }
        if (cause instanceof JobException) {
          log.warn("Unexpected exception while running step", cause);
          throw (JobException) cause;
        }
        log.error("Unexpected exception while running step", cause);
        throw new JobException(cause);
      }
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new OryxConfiguration(), new ParallelStep(), args);
  }

}
