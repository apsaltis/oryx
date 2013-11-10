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

package com.cloudera.oryx.kmeans.computation.normalize;

import com.cloudera.oryx.common.settings.InboundSettings;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import java.io.Serializable;
import java.util.Map;

public final class NormalizeSettings implements Serializable {

  private final Boolean sparse;
  private final Transform defaultTransform;
  private final Map<Integer, Transform> transforms;
  private final Map<Integer, Double> scale;

  public static NormalizeSettings create(Config config) {
    Config normalize = config.getConfig("model.normalize");
    Boolean sparse = normalize.hasPath("sparse") ? normalize.getBoolean("sparse") : null;

    Transform defaultTransform = Transform.forName(normalize.getString("default-transform"));
    Function<Object, Integer> lookup = InboundSettings.create(config).getLookupFunction();
    Map<Integer, Transform> transforms = Maps.newHashMap();
    load(normalize, "z-transform", Transform.Z, lookup, transforms);
    load(normalize, "log-transform", Transform.LOG, lookup, transforms);
    load(normalize, "linear-transform", Transform.LINEAR, lookup, transforms);
    load(normalize, "no-transform", Transform.NONE, lookup, transforms);

    Map<Integer, Double> scale = Maps.newHashMap();
    if (normalize.hasPath("scale")) {
      Config scaleConfig = normalize.getConfig("scale");
      for (Map.Entry<String, ConfigValue> e : scaleConfig.entrySet()) {
        scale.put(lookup.apply(e.getKey()), scaleConfig.getDouble(e.getKey()));
      }
    }

    return new NormalizeSettings(sparse, defaultTransform, transforms, scale);
  }

  private static void load(Config normalize, String path, Transform transform,
                           Function<Object, Integer> lookup,
                           Map<Integer, Transform> output) {
    if (normalize.hasPath(path)) {
      for (Integer column : Lists.transform(normalize.getAnyRefList(path), lookup)) {
        if (output.containsKey(column)) {
          throw new IllegalStateException("Multiple transforms specified for column: " + column);
        }
        output.put(column, transform);
      }
    }
  }

  public NormalizeSettings() {
    this(Transform.NONE);
  }

  public NormalizeSettings(Transform defaultTransform) {
    this(false, defaultTransform, ImmutableMap.<Integer, Transform>of(), ImmutableMap.<Integer, Double>of());
  }

  private NormalizeSettings(Boolean sparse,
                           Transform defaultTransform,
                           Map<Integer, Transform> transforms,
                            Map<Integer, Double> scale) {
    this.sparse = sparse;
    this.defaultTransform = defaultTransform;
    this.transforms = transforms;
    this.scale = scale;
  }

  public Boolean getSparse() {
    return sparse;
  }

  public Transform getTransform(int column) {
    Transform t = transforms.get(column);
    if (t == null) {
      t = defaultTransform;
    }
    return t;
  }

  public double getScale(int column) {
    Double d = scale.get(column);
    if (d == null) {
      return 1.0;
    }
    return d;
  }
}
