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

package com.cloudera.oryx.als.serving.generation;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.DataUtils;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.common.pmml.ALSModelDescription;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;

/**
 * @author Sean Owen
 */
final class GenerationLoader {

  private static final Logger log = LoggerFactory.getLogger(GenerationLoader.class);

  private final String instanceDir;
  private final LongSet recentlyActiveUsers;
  private final LongSet recentlyActiveItems;
  private final Object lockForRecent;
  
  GenerationLoader(String instanceDir,
                   LongSet recentlyActiveUsers,
                   LongSet recentlyActiveItems,
                   Object lockForRecent) {
    this.instanceDir = instanceDir;
    this.recentlyActiveUsers = recentlyActiveUsers;
    this.recentlyActiveItems = recentlyActiveItems;
    // This must be acquired to access the 'recent' fields above:
    this.lockForRecent = lockForRecent;
  }

  void loadModel(long generationID, Generation currentGeneration) throws IOException {

    File modelPMMLFile = File.createTempFile("oryx-model", ".pmml.gz");
    modelPMMLFile.deleteOnExit();
    IOUtils.delete(modelPMMLFile);

    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    String modelPMMLKey = generationPrefix + "model.pmml.gz";
    Store.get().download(modelPMMLKey, modelPMMLFile);
    log.info("Loading model description from {}", modelPMMLKey);

    ALSModelDescription modelDescription = ALSModelDescription.read(modelPMMLFile);
    IOUtils.delete(modelPMMLFile);

    Collection<Future<Object>> futures = Lists.newArrayList();
    // Limit this fairly sharply to 2 so as to not saturate the network link
    ExecutorService executor = Executors.newFixedThreadPool(
        2,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LoadModel-%d").build());

    LongSet loadedUserIDs;
    LongSet loadedItemIDs;
    LongSet loadedUserIDsForKnownItems;
    try {
      loadedUserIDs = loadXOrY(generationPrefix, modelDescription, true, currentGeneration, futures, executor);
      loadedItemIDs = loadXOrY(generationPrefix, modelDescription, false, currentGeneration, futures, executor);

      if (currentGeneration.getKnownItemIDs() == null) {
        loadedUserIDsForKnownItems = null;
      } else {
        loadedUserIDsForKnownItems =
            loadKnownItemIDs(generationPrefix,
                             modelDescription,
                             currentGeneration,
                             futures,
                             executor);
      }

      loadIDMapping(generationPrefix, modelDescription, currentGeneration, futures, executor);

      ExecutorUtils.checkExceptions(futures);

      log.info("Finished all load tasks");
      
    } finally {
      ExecutorUtils.shutdownNowAndAwait(executor);
    }

    synchronized (lockForRecent) {
      log.info("Pruning old entries...");
      removeNotUpdated(currentGeneration.getX().keySetIterator(),
                       loadedUserIDs,
                       recentlyActiveUsers,
                       currentGeneration.getXLock().writeLock());
      removeNotUpdated(currentGeneration.getY().keySetIterator(),
                       loadedItemIDs,
                       recentlyActiveItems,
                       currentGeneration.getYLock().writeLock());
      if (loadedUserIDsForKnownItems != null && currentGeneration.getKnownItemIDs() != null) {
        removeNotUpdated(currentGeneration.getKnownItemIDs().keySetIterator(),
                         loadedUserIDsForKnownItems,
                         recentlyActiveUsers,
                         currentGeneration.getKnownItemLock().writeLock());
      }
      this.recentlyActiveItems.clear();
      this.recentlyActiveUsers.clear();
    }

    log.info("Recomputing generation state...");
    currentGeneration.recomputeState();

    log.info("All model elements loaded, {} users and {} items", 
             currentGeneration.getNumUsers(), currentGeneration.getNumItems());
  }

  private static LongSet loadXOrY(String generationPrefix,
                                    ALSModelDescription modelDescription,
                                    boolean isX,
                                    Generation generation,
                                    Collection<Future<Object>> futures,
                                    ExecutorService executor) throws IOException {

    String xOrYPrefix = generationPrefix + (isX ? modelDescription.getXPath() : modelDescription.getYPath());
    final LongSet loadedIDs = new LongSet();
    
    final Lock writeLock = isX ? generation.getXLock().writeLock() : generation.getYLock().writeLock();
    final LongObjectMap<float[]> xOrYMatrix = isX ? generation.getX() : generation.getY();

    for (final String xOrYFilePrefix : Store.get().list(xOrYPrefix, true)) {
      futures.add(executor.submit(new Callable<Object>() {
        @Override
        public Void call() throws IOException {
          for (String line : new FileLineIterable(Store.get().readFrom(xOrYFilePrefix))) {

            int tab = line.indexOf('\t');
            Preconditions.checkArgument(tab >= 0, "Bad input line in %s: %s", xOrYFilePrefix, line);
            long id = Long.parseLong(line.substring(0, tab));

            float[] elements = DataUtils.readFeatureVector(line.substring(tab + 1));

            writeLock.lock();
            try {
              xOrYMatrix.put(id, elements);
              loadedIDs.add(id);
            } finally {
              writeLock.unlock();
            }
          }
          log.info("Loaded feature vectors from {}", xOrYFilePrefix);
          return null;
        }
      }));
    }

    return loadedIDs;
  }


  private static LongSet loadKnownItemIDs(String generationPrefix,
                                            ALSModelDescription modelDescription,
                                            final Generation generation,
                                            Collection<Future<Object>> futures,
                                            ExecutorService executor) throws IOException {
    final LongSet loadedIDs = new LongSet();
    String knownItemsPrefix = generationPrefix + modelDescription.getKnownItemsPath();
    for (final String knownItemFilePrefix : Store.get().list(knownItemsPrefix, true)) {
      futures.add(executor.submit(new Callable<Object>() {
        @Override
        public Void call() throws IOException {
          for (String line : new FileLineIterable(Store.get().readFrom(knownItemFilePrefix))) {
            int tab = line.indexOf('\t');
            Preconditions.checkArgument(tab >= 0, "Bad input line in %s: %s", knownItemFilePrefix, line);
            long userID = Long.parseLong(line.substring(0, tab));
            LongSet itemIDs = stringToSet(line.substring(tab + 1));
            Lock writeLock = generation.getKnownItemLock().writeLock();
            LongObjectMap<LongSet> knownItems = generation.getKnownItemIDs();
            writeLock.lock();
            try {
              knownItems.put(userID, itemIDs);
              loadedIDs.add(userID);
            } finally {
              writeLock.unlock();
            }
          }
          log.info("Loaded known items from {}", knownItemFilePrefix);
          return null;
        }
      }));
    }
    return loadedIDs;
  }

  private static LongSet stringToSet(CharSequence values) {
    LongSet result = new LongSet();
    for (String valueString : DelimitedDataUtils.decode(values)) {
      result.add(Long.parseLong(valueString));
    }
    return result;
  }

  private static void removeNotUpdated(LongPrimitiveIterator it,
                                       LongSet updated,
                                       LongSet recentlyActive,
                                       Lock writeLock) {
    writeLock.lock();
    try {
      while (it.hasNext()) {
        long id = it.nextLong();
        if (!updated.contains(id) && !recentlyActive.contains(id)) {
          it.remove();
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private static void loadIDMapping(String generationPrefix,
                                    ALSModelDescription modelDescription,
                                    final Generation generation,
                                    Collection<Future<Object>> futures,
                                    ExecutorService executor) throws IOException {
    String idMappingPrefix = generationPrefix + modelDescription.getIDMappingPath();
    for (final String prefix : Store.get().list(idMappingPrefix, true)) {
      futures.add(executor.submit(new Callable<Object>() {
        @Override
        public Void call() throws IOException {
          for (CharSequence line : new FileLineIterable(Store.get().readFrom(prefix))) {
            String[] columns = DelimitedDataUtils.decode(line);
            long numericID = Long.parseLong(columns[0]);
            String id = columns[1];
            Lock writeLock = generation.getKnownItemLock().writeLock();
            StringLongMapping idMapping = generation.getIDMapping();
            writeLock.lock();
            try {
              idMapping.addMapping(id, numericID);
            } finally {
              writeLock.unlock();
            }
          }
          return null;
        }
      }));
    }
  }

}
