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

package com.cloudera.oryx.common.servcomp;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.io.IOUtils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;

/**
 * Tests {@link Store}.
 *
 * @author Sean Owen
 */
public final class StoreTest extends OryxTest {

  @Test
  public void testSize() throws Exception {
    File file = File.createTempFile("testSize", ".txt");
    file.deleteOnExit();
    Files.write("Hello.", file, Charsets.UTF_8);
    assertEquals(6, Store.get().getSize(file.toString()));
  }

  @Test
  public void testSizeRecursive() throws Exception {
    File dir = Files.createTempDir();
    dir.deleteOnExit();
    File file1 = File.createTempFile("testDU1", ".txt", dir);
    file1.deleteOnExit();
    File file2 = File.createTempFile("testDU2", ".txt", dir);
    file2.deleteOnExit();
    Files.write("Hello.", file1, Charsets.UTF_8);
    Files.write("Shalom.", file2, Charsets.UTF_8);
    assertEquals(7, Store.get().getSizeRecursive(file2.toString()));
    assertEquals(13, Store.get().getSizeRecursive(dir.toString()));
    IOUtils.delete(file1);
    IOUtils.delete(file2);
    IOUtils.delete(dir);
  }
}
