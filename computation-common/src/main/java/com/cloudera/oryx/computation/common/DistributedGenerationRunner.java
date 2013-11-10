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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.cloudera.oryx.common.ClassUtils;
import com.cloudera.oryx.common.servcomp.OryxConfiguration;

/**
 * A {@link DistributedGenerationRunner} completely manages execution of one generation's worth of computation on Hadoop.
 * It is a template, overridden with details specific to particular types of jobs, like clustering.
 */
public abstract class DistributedGenerationRunner extends GenerationRunner {

  private static final Logger log = LoggerFactory.getLogger(DistributedGenerationRunner.class);

  @Override
  protected final void waitForJobAlreadyRunning(String instanceDir) throws IOException, InterruptedException {
    Collection<String> runningJobs;
    do {
      runningJobs = find(instanceDir);
      if (!runningJobs.isEmpty()) {
        log.warn("Jobs are already running for instance {}, waiting: {}", instanceDir, runningJobs);
        Thread.sleep(TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES));
      }
    } while (!runningJobs.isEmpty());
  }

  private static Collection<String> find(String instanceDir) throws IOException, InterruptedException {
    Collection<String> result = Lists.newArrayList();
    // This is where we will see Hadoop config problems first, so log extra info
    Cluster cluster;
    try {
      cluster = new Cluster(new OryxConfiguration());
    } catch (IOException ioe) {
      log.error("Unable to init the Hadoop cluster. Check that an MR2, not MR1, cluster is configured.");
      throw ioe;
    }
    try {
      JobStatus[] statuses = cluster.getAllJobStatuses();
      if (statuses != null) {
        for (JobStatus jobStatus : statuses) {
          JobStatus.State state = jobStatus.getState();
          if (state == JobStatus.State.RUNNING || state ==  JobStatus.State.PREP) {
            cluster.getJob(jobStatus.getJobID());
            String jobName = cluster.getJob(jobStatus.getJobID()).getJobName();
            log.info("Found running job {}", jobName);
            if (jobName.startsWith("Oryx-" + instanceDir + '-')) {
              result.add(jobName);
            }
          }
        }
      }
    } finally {
      cluster.close();
    }
    return result;
  }

  @Override
  protected void runSteps() throws IOException, JobException, InterruptedException {

    doPre();

    DependenciesScheduler<Class<? extends JobStep>> scheduler = new DependenciesScheduler<Class<? extends JobStep>>();

    for (Collection<Class<? extends JobStep>> preStepClasses : scheduler.schedule(getPreDependencies())) {
      runSchedule(preStepClasses, buildConfig(0));
    }

    int iterationNumber = readLatestIterationInProgress();
    log.info("Starting from iteration {}", iterationNumber);

    List<Collection<Class<? extends JobStep>>> iterationSchedule = scheduler.schedule(getIterationDependencies());
    boolean converged = false;
    while (!converged) {
      runOneIteration(iterationNumber, iterationSchedule);
      log.info("Finished iteration {}", iterationNumber);
      if (areIterationsDone(iterationNumber)) {
        converged = true;
      } else {
        iterationNumber++;
      }
    }

    for (Collection<Class<? extends JobStep>> postStepClasses : scheduler.schedule(getPostDependencies())) {
      runSchedule(postStepClasses, buildConfig(iterationNumber));
    }

    doPost();
  }

  /**
   * Override to perform logic before any {@link JobStep}s have executed.
   */
  protected void doPre() throws IOException {
    // do nothing
  }

  /**
   * @return {@link List} of {@link DependsOn} dependencies expressing the {@link JobStep}s that need to happen
   *  in the initial phase, before iteration
   */
  protected abstract List<DependsOn<Class<? extends JobStep>>> getPreDependencies();

  /**
   * @return {@link List} of {@link DependsOn} dependencies expressing the {@link JobStep}s that need to happen
   *  during iteration
   */
  protected abstract List<DependsOn<Class<? extends JobStep>>> getIterationDependencies();

  /**
   * @return {@link List} of {@link DependsOn} dependencies expressing the {@link JobStep}s that need to happen
   *  in the final phase, after iteration
   */
  protected abstract List<DependsOn<Class<? extends JobStep>>> getPostDependencies();

  /**
   * @param iteration iteration number, or 0 if there is no iteration context
   * @return {@link JobStepConfig} appropriate to pass to {@link JobStep}
   */
  protected abstract JobStepConfig buildConfig(int iteration);

  /**
   * Override to change how one iteration is run. By default, all {@link JobStep} in
   * {@link #getIterationDependencies()} are run.
   */
  protected void runOneIteration(int iterationNumber, List<Collection<Class<? extends JobStep>>> iterationSchedule)
      throws InterruptedException, JobException, IOException {
    for (Collection<Class<? extends JobStep>> iterationStepClasses : iterationSchedule) {
      runSchedule(iterationStepClasses, buildConfig(iterationNumber));
    }
  }

  /**
   * @return true iff iteration should be considered complete
   */
  protected abstract boolean areIterationsDone(int iterationNumber) throws IOException;

  /**
   * Override to perform logic after all {@link JobStep}s have executed.
   */
  protected void doPost() throws IOException {
    // do nothing
  }

  protected void runSchedule(Collection<Class<? extends JobStep>> parallelStepClasses,
                             JobStepConfig config) throws JobException, InterruptedException, IOException {

    String[] args = config.toArgsArray();

    if (parallelStepClasses.size() > 1) {

      Collection<String> stepClassNames = Lists.newArrayListWithCapacity(parallelStepClasses.size());
      for (Class<? extends Tool> stepClass : parallelStepClasses) {
        stepClassNames.add(stepClass.getName());
      }
      String joinedStepClassNames = Joiner.on(',').join(stepClassNames);
      String[] argsPlusSteps = new String[args.length + 1];
      System.arraycopy(args, 0, argsPlusSteps, 0, args.length);
      argsPlusSteps[argsPlusSteps.length - 1] = joinedStepClassNames;
      ParallelStep step = new ParallelStep();
      addStateSource(step);
      JobStep.run(step, argsPlusSteps);

    } else {

      Class<? extends JobStep> stepClass = parallelStepClasses.iterator().next();
      JobStep step = ClassUtils.loadInstanceOf(stepClass);
      addStateSource(step);
      JobStep.run(step, args);

    }
  }

}
