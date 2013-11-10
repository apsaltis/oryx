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

import com.cloudera.oryx.computation.common.json.JacksonUtils;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.servcomp.StoreUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

public abstract class GenerationRunner implements Callable<Object> {

  private static final Logger log = LoggerFactory.getLogger(GenerationRunner.class);

  private static final long GENERATION_WAIT = TimeUnit.MILLISECONDS.convert(4, TimeUnit.MINUTES);

  private final String instanceDir;
  private long lastGenerationID;
  private long generationID;
  private Date startTime;
  private Date endTime;
  private long startSize;
  private long endSize;
  private boolean isRunning;
  private final Collection<HasState> stateSources;

  protected GenerationRunner() {
    instanceDir = ConfigUtils.getDefaultConfig().getString("model.instance-dir");
    generationID = -1;
    lastGenerationID = -1;
    //stateSources = Lists.newCopyOnWriteArrayList();
    stateSources = new CopyOnWriteArrayList<HasState>();
  }

  protected final String getInstanceDir() {
    return instanceDir;
  }

  protected final long getGenerationID() {
    return generationID;
  }

  protected final long getLastGenerationID() {
    return lastGenerationID;
  }

  protected final void addStateSource(HasState stateSource) {
    stateSources.add(stateSource);
  }

  /**
   * @return current state of execution at a {@link GenerationRunnerState}, or {@code null} if no job has been
   *  started
   */
  public final GenerationRunnerState getState() throws IOException, InterruptedException {
    if (getGenerationID() < 0) {
      return null;
    }
    List<StepState> stepStates = Lists.newArrayList();
    for (HasState state : stateSources) {
      stepStates.addAll(state.getStepStates());
    }
    return new GenerationRunnerState(getGenerationID(), stepStates, isRunning, startTime, endTime);
  }

  /**
   * Overall entry point -- handles initial concerns like establishing state and checking for running jobs.
   */
  @Override
  public final Void call() throws IOException, JobException, InterruptedException {
    isRunning = true;
    startTime = new Date();
    try {
      waitForJobAlreadyRunning(instanceDir);
      log.info("Starting run for instance {}", instanceDir);
      runGeneration();
      return null;
    } finally {
      isRunning = false;
      endTime = new Date();
    }
  }

  /**
   * If possible, detects if a job is running already over this instance and waits for it if so.
   */
  protected abstract void waitForJobAlreadyRunning(String instanceDir) throws IOException, InterruptedException;

  /**
   * Does the work of actually running one generation.
   */
  private void runGeneration() throws IOException, JobException, InterruptedException {

    List<String> generationStrings = StoreUtils.listGenerationsForInstance(instanceDir);

    Store store = Store.get();
    int count = generationStrings.size();

    Config config = ConfigUtils.getDefaultConfig();
    int maxGenerationsToKeep = config.getInt("model.generations.keep");
    if (count > maxGenerationsToKeep) {
      Iterable<String> generationsToDelete = generationStrings.subList(0, count - maxGenerationsToKeep);
      log.info("Deleting old generations: {}", generationsToDelete);
      for (String generationPrefix : generationsToDelete) {
        store.recursiveDelete(generationPrefix);
      }
    }

    long lastDoneGeneration = -1;
    int lastDoneGenerationIndex = -1;
    for (int i = count - 1; i >= 0; i--) {
      long generationID = StoreUtils.parseGenerationFromPrefix(generationStrings.get(i));
      if (store.exists(Namespaces.getGenerationDoneKey(instanceDir, generationID), true)) {
        lastDoneGeneration = generationID;
        lastDoneGenerationIndex = i;
        break;
      }
    }

    lastGenerationID = lastDoneGeneration;

    long generationToMake;
    long generationToWaitFor;
    long generationToRun;

    if (lastDoneGeneration >= 0) {

      log.info("Last complete generation is {}", lastDoneGeneration);
      if (lastDoneGenerationIndex == count - 1) {
        generationToMake = lastDoneGeneration + 1;
        generationToRun = -1;
        generationToWaitFor = -1;
      } else if (lastDoneGenerationIndex == count - 2) {
        generationToRun = StoreUtils.parseGenerationFromPrefix(generationStrings.get(lastDoneGenerationIndex + 1));
        generationToMake = generationToRun + 1;
        generationToWaitFor = generationToMake;
      } else {
        generationToRun = StoreUtils.parseGenerationFromPrefix(generationStrings.get(lastDoneGenerationIndex + 1));
        generationToMake = -1;
        generationToWaitFor = StoreUtils.parseGenerationFromPrefix(generationStrings.get(lastDoneGenerationIndex + 2));
      }

    } else {

      log.info("No complete generations");
      // If nothing is done,
      if (generationStrings.isEmpty()) {
        // and no generations exist, make one
        generationToRun = -1;
        generationToMake = 0;
        generationToWaitFor = -1;
      } else {
        if (count >= 2) {
          // Run current one and open a next generation
          generationToRun = StoreUtils.parseGenerationFromPrefix(generationStrings.get(0));
          generationToMake = -1;
          generationToWaitFor = StoreUtils.parseGenerationFromPrefix(generationStrings.get(1));
        } else {
          generationToRun = StoreUtils.parseGenerationFromPrefix(generationStrings.get(0));
          generationToMake = generationToRun + 1;
          generationToWaitFor = generationToMake;
        }
      }

    }

    Preconditions.checkState((generationToWaitFor < 0) == (generationToRun < 0),
                             "There must either be both a generation to wait for and generation " +
                                 "to run, or neither");

    if (generationToRun >= 0 && !isAnyInputInGeneration(generationToRun)) {
      log.info("No data in generation {}, so not running", generationToRun);
      generationToRun = -1;
      generationToWaitFor = -1;
      generationToMake = -1;
    }

    if (generationToMake < 0) {
      log.info("No need to make a new generation");
    } else {
      log.info("Making new generation {}", generationToMake);
      store.mkdir(Namespaces.getInstanceGenerationPrefix(instanceDir, generationToMake) + "inbound/");
    }

    maybeWaitToRun(instanceDir, generationToWaitFor, generationToRun);

    // Check again: maybe an upload was in progress but failed!
    if (generationToRun >= 0 && !isAnyInputInGeneration(generationToRun)) {
      log.info("No data in generation {}, so not running", generationToRun);
      generationToRun = -1;
    }

    if (generationToRun < 0) {
      log.info("No generation to run");
    } else {
      generationID = generationToRun;
      log.info("Running generation {}", generationID);
      startSize = store.getSizeRecursive(Namespaces.getInstanceGenerationPrefix(instanceDir, generationID));
      storeConfig(config);
      runSteps();
      log.info("Signaling completion of generation {}", generationID);
      store.touch(Namespaces.getGenerationDoneKey(instanceDir, generationID));
      store.recursiveDelete(Namespaces.getTempPrefix(instanceDir, getGenerationID()));
      log.info("Dumping some stats on generation {}", generationID);
      endSize = store.getSizeRecursive(Namespaces.getInstanceGenerationPrefix(instanceDir, generationID));
      dumpStats();
    }
  }

    private void storeConfig(Config config) throws IOException {
    String outputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "computation.conf";
    String configString = config.root().render(ConfigRenderOptions.concise());
    Writer writer = new OutputStreamWriter(Store.get().streamTo(outputKey), Charsets.UTF_8);
    try {
      writer.write(configString);
    } finally {
      writer.close();
    }
  }

  private void dumpStats() throws IOException {
    // First compute the base stats for all GenerationRunners
    Map<String, Object> stats = Maps.newHashMap();
    stats.put("preRunBytes", startSize);
    stats.put("postRunBytes", endSize);

    // Now compute some subclass-specific stats if impelmented
    Map<String, ?> moreStats = collectStats();
    if (moreStats != null) {
      stats.putAll(moreStats);
    }

    // Write to disk as JSON
    ObjectMapper mapper = JacksonUtils.getObjectMapper();
    String statsString = mapper.writeValueAsString(stats);
    String outputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "stats.json";
    Writer writer = new OutputStreamWriter(Store.get().streamTo(outputKey), Charsets.UTF_8);
    try {
      writer.write(statsString);
    } finally {
      writer.close();
    }
  }

  // Override in implementing subclasses to output more stats
  protected Map<String, ?> collectStats() {
    return null;
  }

  private static void maybeWaitToRun(String instanceDir,
                                     long generationToWaitFor,
                                     long generationToRun) throws IOException, InterruptedException {
    if (generationToWaitFor < 0) {
      log.info("No need to wait for a generation");
      return;
    }

    Store store = Store.get();

    if (generationToRun == 0L) {
      // Special case: let generation 0 run immediately
      log.info("Generation 0 may run immediately", generationToWaitFor);
    } else {
      String nextGenerationPrefix =
          Namespaces.getInstanceGenerationPrefix(instanceDir, generationToWaitFor) + "inbound/";
      long lastModified = store.getLastModified(nextGenerationPrefix);
      long now = System.currentTimeMillis();
      if (now > lastModified + GENERATION_WAIT) {
        log.info("Generation {} is old enough to proceed", generationToWaitFor);
      } else {
        // Not long enough, wait
        long toSleepMS = lastModified + GENERATION_WAIT - now;
        log.info("Waiting {}s for data to start uploading to generation {} and then move to {}...",
                 TimeUnit.SECONDS.convert(toSleepMS, TimeUnit.MILLISECONDS),
                 generationToRun,
                 generationToWaitFor);
        Thread.sleep(toSleepMS);
      }
    }

    String uploadingGenerationPrefix =
        Namespaces.getInstanceGenerationPrefix(instanceDir, generationToRun) + "inbound/";

    boolean anyInProgress = true;
    while (anyInProgress) {
      log.info("Waiting for uploads to finish in {}...", uploadingGenerationPrefix);
      anyInProgress = false;
      for (String fileName : store.list(uploadingGenerationPrefix, true)) {
        // .inprogress is our marker for uploading files; _COPYING_ is Hadoop's
        if (fileName.endsWith(".inprogress") || fileName.endsWith("_COPYING_")) {
          long inProgressLastModified = store.getLastModified(fileName);
          if (System.currentTimeMillis() < inProgressLastModified + 5 * GENERATION_WAIT) {
            log.info("At least one upload is in progress ({})", fileName);
            anyInProgress = true;
            break;
          } else {
            log.warn("Stale upload to {}? Deleting and continuing", fileName);
            try {
              store.delete(fileName);
            } catch (IOException ioe) {
              log.info("Could not delete {}", fileName, ioe);
            }
          }
        }
      }
      if (anyInProgress) {
        Thread.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
      }
    }

  }

  private boolean isAnyInputInGeneration(long generationID) throws IOException {
    Store store = Store.get();
    for (String filePrefix :
         store.list(Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "inbound/", true)) {
      if (store.getSize(filePrefix) > 0) {
        return true;
      }
    }
    return false;
  }

  protected final int readLatestIterationInProgress() throws IOException {
    String iterationsPrefix = Namespaces.getIterationsPrefix(instanceDir,  getGenerationID());
    List<String> iterationPaths = Store.get().list(iterationsPrefix, false);
    if (iterationPaths == null || iterationPaths.isEmpty()) {
      return 1;
    }
    String iterationString = StoreUtils.lastNonEmptyDelimited(iterationPaths.get(iterationPaths.size() - 1));
    int iteration = Integer.parseInt(iterationString);
    if (iteration == 0) {
      // Iteration 0 is a fake iteration with initial Y; means we're really on 1
      iteration = 1;
    }
    return iteration;
  }

  protected abstract void runSteps() throws IOException, JobException, InterruptedException;

}
