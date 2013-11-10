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

package com.cloudera.oryx.common.settings;

import com.cloudera.oryx.common.OryxTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import org.junit.Test;

import java.util.Map;

/**
 * Tests {@link InboundSettings}.
 *
 * @author Sean Owen
 */
public final class InboundSettingsTest extends OryxTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNoSetting() {
    InboundSettings.create(ConfigUtils.getDefaultConfig());
  }

  @Test
  public void testDefaults() throws Exception {
    Map<String, Object> mb = ImmutableMap.<String, Object>builder()
        .put("inbound.categorical-columns", ImmutableList.of())
        .build();
    Config conf = overlayConfigOnDefault(mb);
    InboundSettings settings = InboundSettings.create(conf);
    assertTrue(settings.getIdColumns().isEmpty());
    assertTrue(settings.getIgnoredColumns().isEmpty());
    //assertNull(settings.getSpec());
    assertTrue(settings.getCategoricalColumns().isEmpty());
    assertTrue(settings.getNumericColumns().isEmpty());
    assertEquals(Integer.valueOf(1729), settings.getLookupFunction().apply(1729));
    assertNull(settings.getTargetColumn());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testUnknownColumn() {
    InboundSettings settings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    settings.getLookupFunction().apply("foo");
  }

  @Test
  public void testCategoricalSettings() throws Exception {
    Map<String, Object> mb = ImmutableMap.<String, Object>builder()
        .put("inbound.column-names", ImmutableList.of("a", "b", "c", "d", "e"))
        .put("inbound.id-columns", ImmutableList.of("a"))
        .put("inbound.categorical-columns", ImmutableList.of(1, "e"))
        .put("inbound.ignored-columns", ImmutableList.of("c"))
        .build();
    Config conf = overlayConfigOnDefault(mb);
    InboundSettings settings = InboundSettings.create(conf);
    assertEquals(ImmutableList.of(0), ImmutableList.copyOf(settings.getIdColumns()));
    assertEquals(ImmutableList.of(1, 4), ImmutableList.copyOf(settings.getCategoricalColumns()));
    assertEquals(ImmutableList.of(3), ImmutableList.copyOf(settings.getNumericColumns()));
    assertEquals(ImmutableList.of(2), ImmutableList.copyOf(settings.getIgnoredColumns()));
    assertNull(settings.getTargetColumn());

    /*
    Spec spec = settings.getSpec();
    Assert.assertEquals(5, spec.size());
    Assert.assertEquals(DataType.STRING, spec.getField(0).spec().getDataType());
    Assert.assertEquals(DataType.STRING, spec.getField(1).spec().getDataType());
    Assert.assertEquals(DataType.DOUBLE, spec.getField(3).spec().getDataType());
     */
  }

  @Test
  public void testContinuousSettings() throws Exception {
    Map<String, Object> mb = ImmutableMap.<String, Object>builder()
        .put("inbound.column-names", ImmutableList.of("a", "b", "c", "d", "e"))
        .put("inbound.id-columns", ImmutableList.of("a"))
        .put("inbound.numeric-columns", ImmutableList.of(1, "e"))
        .put("inbound.ignored-columns", ImmutableList.of("c"))
        .build();
    Config conf = overlayConfigOnDefault(mb);
    InboundSettings settings = InboundSettings.create(conf);
    assertEquals(ImmutableList.of(0), ImmutableList.copyOf(settings.getIdColumns()));
    assertEquals(ImmutableList.of(3), ImmutableList.copyOf(settings.getCategoricalColumns()));
    assertEquals(ImmutableList.of(1, 4), ImmutableList.copyOf(settings.getNumericColumns()));
    assertEquals(ImmutableList.of(2), ImmutableList.copyOf(settings.getIgnoredColumns()));
    assertNull(settings.getTargetColumn());

    /*
    Spec spec = settings.getSpec();
    Assert.assertEquals(5, spec.size());
    Assert.assertEquals(DataType.STRING, spec.getField(0).spec().getDataType());
    Assert.assertEquals(DataType.DOUBLE, spec.getField(1).spec().getDataType());
    Assert.assertEquals(DataType.STRING, spec.getField(2).spec().getDataType());
     */
  }

  @Test
  public void testTarget() throws Exception {
    Map<String, Object> mb = ImmutableMap.<String, Object>builder()
        .put("inbound.column-names", ImmutableList.of("a", "e", "c"))
        .put("inbound.categorical-columns", ImmutableList.of("a", "c"))
        .put("inbound.target-column", "c")
        .build();
    Config conf = overlayConfigOnDefault(mb);
    InboundSettings settings = InboundSettings.create(conf);
    assertEquals(2, settings.getTargetColumn().intValue());

  }

}
