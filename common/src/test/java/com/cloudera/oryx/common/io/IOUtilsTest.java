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

package com.cloudera.oryx.common.io;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import com.cloudera.oryx.common.OryxTest;
import com.google.common.io.Files;
import org.junit.Test;

/**
 * Tests {@link IOUtils}.
 *
 * @author Sean Owen
 */
public final class IOUtilsTest extends OryxTest {

  private static final byte[] SOME_BYTES = { 0x01, 0x02, 0x03 };

  @Test
  public void testCopyStream() throws IOException {
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    File subFile1 = new File(tempDir, "subFile1");
    Files.write(SOME_BYTES, subFile1);
    File subFile2 = new File(tempDir, "subFile2");
    IOUtils.copyURLToFile(subFile1.toURI().toURL(), subFile2);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Files.copy(subFile2, baos);
    assertArrayEquals(SOME_BYTES, baos.toByteArray());
  }

  @Test
  public void testDeleteRecursively() throws IOException {
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    assertTrue(tempDir.exists());
    File subFile1 = new File(tempDir, "subFile1");
    Files.write(SOME_BYTES, subFile1);
    assertTrue(subFile1.exists());
    File subDir1 = new File(tempDir, "subDir1");
    IOUtils.mkdirs(subDir1);
    File subFile2 = new File(subDir1, "subFile2");
    Files.write(SOME_BYTES, subFile2);
    assertTrue(subFile2.exists());
    File subDir2 = new File(subDir1, "subDir2");
    IOUtils.mkdirs(subDir2);

    IOUtils.deleteRecursively(tempDir);

    assertFalse(tempDir.exists());
    assertFalse(subFile1.exists());
    assertFalse(subDir1.exists());
    assertFalse(subFile2.exists());
    assertFalse(subDir2.exists());

  }

}
