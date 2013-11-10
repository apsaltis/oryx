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

package com.cloudera.oryx.computation;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.cloudera.oryx.kmeans.computation.local.KMeansLocalGenerationRunner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.computation.ALSDistributedGenerationRunner;
import com.cloudera.oryx.als.computation.local.ALSLocalGenerationRunner;
import com.cloudera.oryx.common.ClassUtils;
import com.cloudera.oryx.common.ReloadingReference;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.servcomp.StoreUtils;
import com.cloudera.oryx.computation.common.GenerationRunner;
import com.cloudera.oryx.computation.common.GenerationRunnerState;
import com.cloudera.oryx.computation.common.JobException;
import com.cloudera.oryx.kmeans.computation.KMeansDistributedGenerationRunner;
import com.cloudera.oryx.rdf.computation.RDFDistributedGenerationRunner;
import com.cloudera.oryx.rdf.computation.local.RDFLocalGenerationRunner;

/**
 * <p>This class will periodically run one generation of the Computation Layer.
 * It can run after a period of time has elapsed, or an amount of data has been written.</p>
 *
 * @author Sean Owen
 */
public final class PeriodicRunner implements Runnable, Closeable {
  // Implement Runnable, not Callable, for ScheduledExecutor

  private static final Logger log = LoggerFactory.getLogger(PeriodicRunner.class);

  private static final Map<String,List<Class<? extends GenerationRunner>>> RUNNERS;
  static {
    List<Class<? extends GenerationRunner>> alsRunners = Lists.newArrayListWithCapacity(2);
    alsRunners.add(ALSLocalGenerationRunner.class);
    alsRunners.add(ALSDistributedGenerationRunner.class);
    List<Class<? extends GenerationRunner>> kmeansRunners = Lists.newArrayListWithCapacity(2);
    kmeansRunners.add(KMeansLocalGenerationRunner.class);
    kmeansRunners.add(KMeansDistributedGenerationRunner.class);
    List<Class<? extends GenerationRunner>> rdfRunners = Lists.newArrayListWithCapacity(2);
    rdfRunners.add(RDFLocalGenerationRunner.class);
    rdfRunners.add(RDFDistributedGenerationRunner.class);
    RUNNERS = ImmutableMap.of("als", alsRunners, "kmeans", kmeansRunners, "rdf", rdfRunners);
  }

  private static final long CHECK_INTERVAL_MINS = 1;

  private final Config config;
  private long lastUpdateTime;
  private long nextScheduledRunTime;
  private boolean forceRun;
  private boolean running;
  private final Collection<GenerationRunner> allGenerationRunners;
  private final ScheduledExecutorService executor;
  private Future<?> future;
  private final ReloadingReference<PeriodicRunnerState> cachedState;

  public PeriodicRunner() {

    this.config = ConfigUtils.getDefaultConfig();

    lastUpdateTime = System.currentTimeMillis();
    int threshold = config.getInt("model.time-threshold");
    if (threshold < 0) {
      nextScheduledRunTime = Long.MAX_VALUE;
    } else {
      nextScheduledRunTime = lastUpdateTime + TimeUnit.MILLISECONDS.convert(threshold, TimeUnit.MINUTES);
    }

    forceRun = false;
    running = false;

    //this.allGenerationRunners = Lists.newCopyOnWriteArrayList();
    this.allGenerationRunners = new CopyOnWriteArrayList<GenerationRunner>();

    executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("PeriodicRunner-%d").build());
    future = executor.scheduleWithFixedDelay(this, 0L, CHECK_INTERVAL_MINS, TimeUnit.MINUTES);

    this.cachedState = new ReloadingReference<PeriodicRunnerState>(new Callable<PeriodicRunnerState>() {
      @Override
      public PeriodicRunnerState call() throws IOException, InterruptedException {
        return doGetState();
      }
    }, 15, TimeUnit.SECONDS);
  }

  public PeriodicRunnerState getState() {
    return cachedState.get(3, TimeUnit.SECONDS);
  }

  private PeriodicRunnerState doGetState() throws IOException, InterruptedException {

    List<GenerationRunnerState> states = Lists.newArrayListWithCapacity(allGenerationRunners.size());
    for (GenerationRunner runner : allGenerationRunners) {
      GenerationRunnerState runnerSate = runner.getState();
      if (runnerSate != null) {
        states.add(runnerSate);
      }
    }
    Collections.reverse(states);
    Date nextScheduledRunTimeDate = nextScheduledRunTime == Long.MAX_VALUE ? null : new Date(nextScheduledRunTime);

    List<String> mostRecentGenerations = StoreUtils.listGenerationsForInstance(config.getString("model.instance-dir"));
    int currentGenerationMB = dataInCurrentGenerationMB(mostRecentGenerations);

    return new PeriodicRunnerState(states, running, nextScheduledRunTimeDate, currentGenerationMB);
  }

  public void forceRun() {
    forceRun = true;
    future.cancel(true);
    future = executor.scheduleWithFixedDelay(this, 0L, CHECK_INTERVAL_MINS, TimeUnit.MINUTES);
  }

  public void interrupt() {
    future.cancel(true);
    future = executor.scheduleWithFixedDelay(this, CHECK_INTERVAL_MINS, CHECK_INTERVAL_MINS, TimeUnit.MINUTES);
  }

  @Override
  public void close() {
    ExecutorUtils.shutdownNowAndAwait(executor);
    future.cancel(true);
  }

  @Override
  public void run()  {

    try {
      running = true;
      long now = System.currentTimeMillis();

      boolean timeThresholdExceeded = now >= nextScheduledRunTime;

      String instanceDir = config.getString("model.instance-dir");
      List<String> mostRecentGenerations = StoreUtils.listGenerationsForInstance(instanceDir);

      if (!isAnyCompleteGeneration(instanceDir, mostRecentGenerations)) {
        log.info("Forcing run -- no complete generations yet");
        forceRun = true;
      }

      int currentGenerationMB = dataInCurrentGenerationMB(mostRecentGenerations);
      int dataThresholdMB = config.getInt("model.data-threshold");
      boolean dataThresholdExceeded =
          dataThresholdMB > 0 && currentGenerationMB >= dataThresholdMB;

      if (timeThresholdExceeded) {
        log.info("Running new generation due to elapsed time: {} minutes",
                 TimeUnit.MINUTES.convert(now - lastUpdateTime, TimeUnit.MILLISECONDS));
      }
      if (dataThresholdExceeded) {
        log.info("Running new generation due to data written: {}MB", currentGenerationMB);
      }

      if (forceRun || timeThresholdExceeded || dataThresholdExceeded) {
        forceRun = false;
        GenerationRunner newGenerationRunner = buildGenerationRunner();
        allGenerationRunners.add(newGenerationRunner);

        // If time threshold was exceeded, pretend we started the run at the scheduled time. We noticed
        // up to a minute later, but conceptually it started then. Otherwise the start time is
        // conceptually now (due to data)
        if (now > nextScheduledRunTime) {
          lastUpdateTime = nextScheduledRunTime;
        } else {
          lastUpdateTime = now;
        }
        int minBetweenRuns = config.getInt("model.time-threshold");
        if (minBetweenRuns < 0) {
          nextScheduledRunTime = Long.MAX_VALUE;
        } else {
          long msBetweenRuns = TimeUnit.MILLISECONDS.convert(minBetweenRuns, TimeUnit.MINUTES);
          nextScheduledRunTime = lastUpdateTime + msBetweenRuns;
        }

        newGenerationRunner.call();

        // If we ran over time limit, schedule will run again immediately. For bookkeeping correctness,
        // move back the scheduled run time to now, to reflect the fact that it was preempted.
        long endTime = System.currentTimeMillis();
        if (endTime > nextScheduledRunTime) {
          nextScheduledRunTime = endTime;
        }
      }

    } catch (InterruptedException ignored) {
      log.warn("Interrupted");
    } catch (IOException ioe) {
      log.warn("Unexpected error in execution", ioe);
    } catch (JobException je) {
      log.warn("Unexpected error in execution", je);
    } catch (Throwable t) {
      log.error("Unexpected error in execution", t);
    } finally {
      running = false;
    }
  }

  private static boolean isAnyCompleteGeneration(String instanceDir,
                                                 Collection<String> mostRecentGenerations) throws IOException {
    if (mostRecentGenerations.isEmpty()) {
      return false;
    }
    Store store = Store.get();
    for (CharSequence recentGenerationPathString : mostRecentGenerations) {
      long generationID = StoreUtils.parseGenerationFromPrefix(recentGenerationPathString);
      if (store.exists(Namespaces.getGenerationDoneKey(instanceDir, generationID), true)) {
        return true;
      }
    }
    return false;
  }

  private int dataInCurrentGenerationMB(List<String> mostRecentGenerations) throws IOException {
    if (mostRecentGenerations.isEmpty()) {
      return 0;
    }
    long currentGenerationID =
        StoreUtils.parseGenerationFromPrefix(mostRecentGenerations.get(mostRecentGenerations.size() - 1));
    String inboundPrefix =
        Namespaces.getInstanceGenerationPrefix(config.getString("model.instance-dir"), currentGenerationID) +
            "inbound/";

    Store store = Store.get();
    long totalSizeBytes = 0L;
    for (String filePrefix : store.list(inboundPrefix, true)) {
      totalSizeBytes += store.getSize(filePrefix);
    }
    return (int) (totalSizeBytes / 1000000);
  }

  private GenerationRunner buildGenerationRunner() {
    List<Class<? extends GenerationRunner>> localDistributed = RUNNERS.get(config.getString("model.type"));
    Preconditions.checkNotNull(localDistributed,
                               "Unspecified or unsupported model type: %s",
                               config.getString("model.type"));
    boolean local = config.getBoolean("model.local");
    Class<? extends GenerationRunner> runnerClass = local ? localDistributed.get(0) : localDistributed.get(1);
    return ClassUtils.loadInstanceOf(runnerClass);
  }

}
