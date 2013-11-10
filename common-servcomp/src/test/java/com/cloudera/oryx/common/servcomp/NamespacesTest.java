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
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests {@link Namespaces}.
 *
 * @author Sean Owen
 */
public final class NamespacesTest extends OryxTest {

  @Test
  public void testNamespaces() throws Exception {
    Config config = ConfigUtils.getDefaultConfig();
    assertEquals("/tmp/OryxTest/00001/",
                 Namespaces.getInstanceGenerationPrefix(config.getString("model.instance-dir"), 1L));
    assertEquals("/tmp/OryxTest/00001/tmp/iterations/",
                 Namespaces.getIterationsPrefix(config.getString("model.instance-dir"), 1L));
    assertEquals("/tmp/OryxTest/00001/_SUCCESS",
                 Namespaces.getGenerationDoneKey(config.getString("model.instance-dir"), 1L));
    assertEquals(new Path("file:/user/smurf"),
                 Namespaces.toPath("/user/smurf"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeGenerationID() throws Exception {
    Namespaces.getInstanceGenerationPrefix(ConfigUtils.getDefaultConfig().getString("model.instance-dir"), -1L);
  }

}
