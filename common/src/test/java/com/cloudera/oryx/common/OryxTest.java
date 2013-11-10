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

package com.cloudera.oryx.common;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.log.MemoryHandler;
import com.cloudera.oryx.common.random.RandomManager;

import java.net.URISyntaxException;
import java.util.Map;

/**
 * Superclass of all tests.
 */
public abstract class OryxTest extends Assert {

  private static final float FLOAT_EPSILON = 1.0e-6f;
  private static final double DOUBLE_EPSILON = 1.0e-12;
  protected static final File TEST_TEMP_BASE_DIR = new File("/tmp/OryxTest");

  private boolean resourceExists(String resourceBasename) {
    return getClass().getClassLoader().getResource(resourceBasename) != null;
  }

  /**
   * @return true iff the argument exists, is a directory, and contains at least one non-hidden file
   */
  protected static boolean isDirectoryWithFiles(File dir) {
    if (!dir.exists() || !dir.isDirectory()) {
      return false;
    }
    File[] files = dir.listFiles(IOUtils.NOT_HIDDEN);
    return files != null && files.length > 0;
  }

  protected final File getResourceAsFile(String resourceBasename) {
    if (resourceExists(resourceBasename)) {
      try {
        return new File(Resources.getResource(resourceBasename).toURI());
      } catch (URISyntaxException e) {
        throw new IllegalStateException("Invalid/non-existent resource: " + resourceBasename, e);
      }
    }
    // Fallback for local runs -- use file in file system
    return new File("src/it/resources", resourceBasename);
  }

  /**
   * Asserts that two {@code float} values are equal or very close -- that the absolute value of their
   * difference is at most {@link #FLOAT_EPSILON}.
   *
   * @param expected test's expected value
   * @param actual actual value
   */
  protected static void assertEquals(float expected, float actual) {
    Assert.assertEquals(expected, actual, FLOAT_EPSILON);
  }

  protected static void assertArrayEquals(float[] expecteds, float[] actuals) {
    Assert.assertArrayEquals(expecteds, actuals, FLOAT_EPSILON);
  }

  /**
   * Asserts that two {@code double} values are equal or very close -- that the absolute value of their
   * difference is at most {@link #DOUBLE_EPSILON}.
   *
   * @param expected test's expected value
   * @param actual actual value
   */
  @SuppressWarnings("deprecation")
  public static void assertEquals(double expected, double actual) {
    Assert.assertEquals(expected, actual, DOUBLE_EPSILON);
  }

  protected static void assertArrayEquals(double[] expecteds, double[] actuals) {
    Assert.assertArrayEquals(expecteds, actuals, DOUBLE_EPSILON);
  }

  public static void assertArrayEquals(String message, double[] expecteds, double[] actuals) {
    Assert.assertArrayEquals(message, expecteds, actuals, DOUBLE_EPSILON);
  }

  protected static void assertArrayEquals(float[] expecteds, double[] actuals) {
    float[] actualFloats = new float[actuals.length];
    for (int i = 0; i < actuals.length; i++) {
      actualFloats[i] = (float) actuals[i];
    }
    assertArrayEquals(expecteds, actualFloats);
  }

  /**
   * Overlays some key-value pairs manually on the global configuration.
   *
   * @param props key-value pairs to add to global configuration
   * @return global {@link Config} with key-value pairs added
   */
  public static Config overlayConfigOnDefault(Map<String, Object> props) {
    return ConfigFactory.parseMap(props).resolve().withFallback(ConfigUtils.getDefaultConfig());
  }

  @BeforeClass
  public static void setUpClass() {
    MemoryHandler.setSensibleLogFormat();
  }

  @Before
  public void setUp() throws Exception {
    IOUtils.deleteRecursively(TEST_TEMP_BASE_DIR);
    IOUtils.mkdirs(TEST_TEMP_BASE_DIR);
    RandomManager.useTestSeed();
    // simulate specifying -Dconfig.file=oryx-test.conf
    ConfigUtils.loadUserConfig(getTestConfigResource());
  }

  /**
   * @return name of configuration {@code .conf} file which should be read in to configure the application
   *  for purposes of this test.
   */
  protected String getTestConfigResource() {
    return "oryx-test.conf";
  }

  @After
  public void tearDown() throws Exception {
    IOUtils.deleteRecursively(TEST_TEMP_BASE_DIR);
  }

  /**
   * Asserts that a {@code double} is {@link Double#NaN}.
   */
  protected static void assertNaN(double d) {
    assertTrue("Expected NaN but got " + d, Double.isNaN(d));
  }

  /**
   * Asserts that a {@code float} is {@link Float#NaN}.
   */
  protected static void assertNaN(float f) {
    assertTrue("Expected NaN but got " + f, Float.isNaN(f));
  }

}
