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

package com.cloudera.oryx.computation.common.fn;

import com.cloudera.oryx.common.servcomp.OryxConfiguration;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStep;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OryxDoFn<S, T> extends DoFn<S, T> {
  private static final Logger log = LoggerFactory.getLogger(OryxDoFn.class);

  private OryxConfiguration configuration;

  @Override
  public final OryxConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public void initialize() {
    super.initialize();
    Configuration rawConfiguration = super.getConfiguration();
    log.info("Setup of {} with config {}", this, rawConfiguration);
    this.configuration = new OryxConfiguration(rawConfiguration);
    ConfigUtils.overlayConfigOnDefault(configuration.get(JobStep.CONFIG_SERIALIZATION_KEY));
  }

  @Override
  public void cleanup(Emitter<T> emitter) {
    log.info("Cleanup of {}", this);
    super.cleanup(emitter);
  }

  @Override
  public final String toString() {
    return getClass().getSimpleName();
  }
}
