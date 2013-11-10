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

package com.cloudera.oryx.serving.generation;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.servcomp.StoreUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * An implementation of {@link GenerationManager} is responsible for interacting with successive generations of the
 * underlying model. It sends updates to the component responsible for computing the model,
 * and manages switching in new models when they become available.
 *
 * @author Sean Owen
 */
public abstract class GenerationManager implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(GenerationManager.class);

  protected static final long NO_GENERATION = Long.MIN_VALUE;

  private final String instanceDir;
  private final ScheduledExecutorService executorService;
  private long writeGeneration;
  private Writer appender;
  private final File appendTempDir;
  private File appenderTempFile;
  private final long writesBetweenUpload;
  private long countdownToUpload;
  private final Semaphore refreshSemaphore;

  protected GenerationManager(File appendTempDir) throws IOException {

    Config config = ConfigUtils.getDefaultConfig();

    this.instanceDir = config.getString("model.instance-dir");
    this.appendTempDir = appendTempDir;
    writesBetweenUpload = config.getLong("model.writes-between-upload");
    countdownToUpload = writesBetweenUpload;

    writeGeneration = NO_GENERATION;

    executorService = Executors.newScheduledThreadPool(3, new ThreadFactoryBuilder().setDaemon(true).build());
    refreshSemaphore = new Semaphore(1);

    executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (!executorService.isShutdown()) {
          try {
            maybeRollAppender();
          } catch (Throwable t) {
            log.warn("Exception while maybe rolling appender", t);
          }
        }
      }
    }, 2, 2, TimeUnit.MINUTES);
    maybeRollAppender();

    executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (!executorService.isShutdown()) {
          try {
            refresh();
          } catch (Throwable t) {
            log.warn("Exception while refreshing", t);
          }
        }
      }
    }, 0, 7, TimeUnit.MINUTES); // Should be mutually prime with delay set above
  }

  protected final synchronized Writer getAppender() {
    return appender;
  }

  private synchronized void closeAppender() {
    if (appender != null) {

      if (writeGeneration < 0L) {
        log.warn("No write generation to upload file to; aborting upload and waiting for later");
        return;
      }

      try {
        appender.close();
      } catch (IOException ioe) {
        log.warn("Unable to close {} ({}); aborting and deleting file", appenderTempFile, ioe.toString());
        appender = null;
        try {
          IOUtils.delete(appenderTempFile);
        } catch (IOException e) {
          log.warn("Failed to delete {}", appenderTempFile);
        }
        appenderTempFile = null;
        return;
      }

      appender = null;

      final File fileToUpload = appenderTempFile;
      appenderTempFile = null;

      boolean fileToUploadHasData;
      try {
        fileToUploadHasData = fileToUpload.exists() && !IOUtils.isGZIPFileEmpty(fileToUpload);
      } catch (IOException ioe) {
        log.warn("Unexpected error checking {} for data; deleting", fileToUpload, ioe);
        try {
          IOUtils.delete(fileToUpload);
        } catch (IOException e) {
          log.warn("Failed to delete {}", fileToUpload);
        }
        fileToUploadHasData = false;
      }

      if (fileToUploadHasData) {

        final String appendKey =
            Namespaces.getInstanceGenerationPrefix(instanceDir, writeGeneration) + "inbound/" + fileToUpload.getName();

        Callable<?> uploadCallable = new Callable<Object>() {
          @Override
          public Void call() {
            log.info("Uploading {} to {}", fileToUpload, appendKey);
            String appendProgressKey = appendKey + ".inprogress";
            try {
              Store store = Store.get();
              store.touch(appendProgressKey);
              store.upload(appendKey, fileToUpload, false);
              log.info("Uploaded to {}", appendKey);
            } catch (Throwable t) {
              log.warn("Unable to upload {} Continuing...", fileToUpload);
              log.warn("Exception was:", t);
            } finally {
              try {
                Store.get().delete(appendProgressKey);
                IOUtils.delete(fileToUpload);
              } catch (IOException e) {
                log.warn("Could not delete {}", appendProgressKey, e);
              }
            }
            return null;
          }
        };

        if (executorService.isShutdown()) {
          // Occurring during shutdown, so can't handle exceptions or use the executor
          try {
            uploadCallable.call();
          } catch (Exception e) {
            log.warn("Unexpected error while trying to upload file during shutdown", e);
          }
        } else {
          Preconditions.checkNotNull(executorService.submit(uploadCallable));
        }

      } else {
        // Just delete right away
        log.info("File appears to have no data, deleting: {}", fileToUpload);
        try {
          IOUtils.delete(fileToUpload);
        } catch (IOException e) {
          log.warn("Failed to delete {}", appenderTempFile);
        }
      }
    }
  }

  protected final synchronized void decrementCountdownToUpload() {
    countdownToUpload--;
  }

  private synchronized void maybeRollAppender() throws IOException {
    long newMostRecentGeneration = getMostRecentGeneration();
    if (newMostRecentGeneration > writeGeneration || countdownToUpload <= 0) {
      countdownToUpload = writesBetweenUpload;
      // Close and write into *current* write generation first -- but only if it exists
      if (writeGeneration >= 0L) {
        closeAppender();
        writeGeneration = newMostRecentGeneration;
      } else {
        writeGeneration = newMostRecentGeneration;
        closeAppender();
      }
      appenderTempFile = File.createTempFile("oryx-append-", ".csv.gz", appendTempDir);
      // A small buffer is needed here, but GZIPOutputStream already provides a substantial native buffer
      appender = IOUtils.buildGZIPWriter(appenderTempFile);
    }
  }

  private long getMostRecentGeneration() throws IOException {
    List<String> recentGenerationPathStrings = StoreUtils.listGenerationsForInstance(instanceDir);
    if (recentGenerationPathStrings.isEmpty()) {
      log.warn("No generation found at all at {}; wrong path?", Namespaces.getInstancePrefix(instanceDir));
      return -1L;
    }
    return StoreUtils.parseGenerationFromPrefix(
        recentGenerationPathStrings.get(recentGenerationPathStrings.size() - 1));
  }

  @Override
  public final synchronized void close() {
    ExecutorUtils.shutdownAndAwait(executorService); // Let others complete
    closeAppender();
  }

  /**
   * Triggers a refresh of the object's internal state, which particularly includes rebuilding or reloading
   * a matrix model.
   */
  public final synchronized void refresh() {
    try {
      if (appender != null) {
        appender.flush();
      }
    } catch (IOException e) {
      log.warn("Exception while flushing", e);
    }

    if (refreshSemaphore.tryAcquire()) {
      Preconditions.checkNotNull(executorService.submit(new RefreshCallable()));
    } else {
      log.info("Refresh is already in progress");
    }
  }

  protected abstract void loadRecentModel(long mostRecentModelGeneration) throws IOException;

  private final class RefreshCallable implements Callable<Object> {

    @Override
    public Void call() {
      try {
        maybeRollAppender();
        long mostRecentModelGeneration = getMostRecentModelGeneration();
        if (mostRecentModelGeneration >= 0L) {
          loadRecentModel(mostRecentModelGeneration);
        } else {
          log.info("No available generation, nothing to do");
        }
      } catch (Throwable t) {
        log.warn("Unexpected exception while refreshing", t);
      } finally {
        refreshSemaphore.release();
      }
      return null;
    }

    private long getMostRecentModelGeneration() throws IOException {
      List<String> recentGenerationPathStrings = StoreUtils.listGenerationsForInstance(instanceDir);
      Store store = Store.get();
      for (int i = recentGenerationPathStrings.size() - 1; i >= 0; i--) {
        long generationID = StoreUtils.parseGenerationFromPrefix(recentGenerationPathStrings.get(i));
        String instanceGenerationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
        if (store.exists(Namespaces.getGenerationDoneKey(instanceDir, generationID), true) &&
            store.exists(instanceGenerationPrefix + "model.pmml.gz", true)) {
          return generationID;
        }
      }
      return -1L;
    }

  }

}
