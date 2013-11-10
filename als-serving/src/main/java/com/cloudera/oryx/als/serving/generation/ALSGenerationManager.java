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
import java.io.Writer;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.math.SolverException;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.serving.generation.GenerationManager;

/**
 * An implementation of {@link ALSGenerationManager} is responsible for interacting with successive generations of the
 * underlying recommender model. It sends updates to the component responsible for computing the model,
 * and manages switching in new models when they become available.
 * 
 * @author Sean Owen
 */
public final class ALSGenerationManager extends GenerationManager {

  private static final Logger log = LoggerFactory.getLogger(ALSGenerationManager.class);

  private long modelGeneration;
  private Generation currentGeneration;
  private final LongSet recentlyActiveUsers;
  private final LongSet recentlyActiveItems;
  private final GenerationLoader loader;

  public ALSGenerationManager(File appendTempDir) throws IOException {
    super(appendTempDir);
    modelGeneration = NO_GENERATION;
    recentlyActiveUsers = new LongSet();
    recentlyActiveItems = new LongSet();
    Config config = ConfigUtils.getDefaultConfig();
    loader = new GenerationLoader(config.getString("model.instance-dir"), recentlyActiveUsers, recentlyActiveItems, this);
  }

  /**
   * @return an instance of the latest {@link Generation} that has been made available by the
   * implementation.
   */
  public synchronized Generation getCurrentGeneration() {
    return currentGeneration;
  }

  /**
   * Sends a new user / item association to the component responsible for later recomputing
   * the model based on this, and other, updates.
   *
   * @param userID user involved in new association
   * @param itemID item involved
   * @param value strength of the user/item association
   * @throws IOException if an error occurs while sending the update
   */
  public void append(String userID, String itemID, float value) throws IOException {
    StringBuilder line = new StringBuilder(32);
    line.append(DelimitedDataUtils.encode(userID, itemID, Float.toString(value))).append('\n');
    doAppend(line, userID, itemID);
  }

  /**
   * Records that the user-item association should be removed. This is different from recording a
   * negative association.
   *
   * @param userID user involved in new association
   * @param itemID item involved
   * @throws IOException if an error occurs while sending the update
   */
  public void remove(String userID, String itemID) throws IOException {
    StringBuilder line = new StringBuilder(32);
    line.append(DelimitedDataUtils.encode(userID, itemID, "")).append('\n');
    doAppend(line, userID, itemID);
  }

  private synchronized void doAppend(CharSequence line, String userID, String itemID) throws IOException {
    Writer appender = getAppender();
    if (appender != null) {
      appender.append(line);
    }
    // User ID reverse mapping is not important to serving layer
    long numericUserID = StringLongMapping.toLong(userID);
    long numericItemID;
    if (currentGeneration == null) {
      numericItemID = StringLongMapping.toLong(itemID);
    } else {
      numericItemID = currentGeneration.getIDMapping().add(itemID);
    }
    recentlyActiveUsers.add(numericUserID);
    recentlyActiveItems.add(numericItemID);
    decrementCountdownToUpload();
  }

  @Override
  protected void loadRecentModel(long mostRecentModelGeneration) throws IOException {
    if (mostRecentModelGeneration <= modelGeneration) {
      return;
    }
    if (modelGeneration == NO_GENERATION) {
      log.info("Most recent generation {} is the first available one", mostRecentModelGeneration);
    } else {
      log.info("Most recent generation {} is newer than current {}",
               mostRecentModelGeneration, modelGeneration);
    }
    try {

      Generation theCurrentGeneration = currentGeneration;
      if (theCurrentGeneration == null) {
        theCurrentGeneration = new Generation();
      }

      loader.loadModel(mostRecentModelGeneration, theCurrentGeneration);

      modelGeneration = mostRecentModelGeneration;
      currentGeneration = theCurrentGeneration;

    } catch (OutOfMemoryError oome) {
      log.warn("Increase heap size with -Xmx, decrease new generation size with larger " +
                   "-XX:NewRatio value, and/or use -XX:+UseCompressedOops");
      currentGeneration = null;
      throw oome;
    } catch (SolverException ignored) {
      log.warn("Unable to compute a valid generation yet; waiting for more data");
      currentGeneration = null;
    }
  }

}
