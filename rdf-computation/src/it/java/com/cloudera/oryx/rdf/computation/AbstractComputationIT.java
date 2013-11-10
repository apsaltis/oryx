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

package com.cloudera.oryx.rdf.computation;

import com.google.common.io.Files;
import org.junit.Assume;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Superclass of various tests that exercise the Computation Layer ALS code.
 * 
 * @author Sean Owen
 */
public abstract class AbstractComputationIT extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(AbstractComputationIT.class);

  static final File TEST_TEMP_INBOUND_DIR = new File(OryxTest.TEST_TEMP_BASE_DIR, "00000/inbound");

  protected abstract File getTestDataPath();

  @Override
  protected String getTestConfigResource() {
    return "AbstractComputationIT.conf";
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    File testDataDir = getTestDataPath();
    Assume.assumeTrue("Skipping test because data is not present", isDirectoryWithFiles(testDataDir));

    IOUtils.mkdirs(TEST_TEMP_INBOUND_DIR);
    log.info("Copying files to {}", TEST_TEMP_INBOUND_DIR);

    File[] srcDataFiles = testDataDir.listFiles(IOUtils.NOT_HIDDEN);
    for (File srcDataFile : srcDataFiles) {
      File destFile = new File(TEST_TEMP_INBOUND_DIR, srcDataFile.getName());
      Files.copy(srcDataFile, destFile);
    }

    ConfigUtils.overlayConfigOnDefault(getResourceAsFile(getClass().getSimpleName() + ".conf"));

  }

}
