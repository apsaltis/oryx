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

package com.cloudera.oryx.als.common.pmml;

import com.cloudera.oryx.common.OryxTest;
import org.junit.Test;

import java.io.File;

/**
 * Tests {@link ALSModelDescription}.
 *
 * @author Sean Owen
 */
public final class ALSModelDescriptionTest extends OryxTest {

  @Test
  public void testWriteRead() throws Exception {
    ALSModelDescription amd = new ALSModelDescription();
    amd.setIDMappingPath("idMapping");
    amd.setKnownItemsPath("knownItems");
    amd.setXPath("xPath");
    amd.setYPath("yPath");

    File tmp = File.createTempFile("als", ".pmml.gz");
    tmp.deleteOnExit();

    ALSModelDescription.write(tmp, amd);
    ALSModelDescription ret = ALSModelDescription.read(tmp);
    assertEquals(amd, ret);
  }

}
