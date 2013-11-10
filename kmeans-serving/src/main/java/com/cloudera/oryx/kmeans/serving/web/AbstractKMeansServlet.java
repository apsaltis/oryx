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

package com.cloudera.oryx.kmeans.serving.web;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import com.cloudera.oryx.kmeans.serving.generation.KMeansGenerationManager;
import com.cloudera.oryx.serving.web.AbstractOryxServlet;

public abstract class AbstractKMeansServlet extends AbstractOryxServlet {

  private static final String KEY_PREFIX = AbstractKMeansServlet.class.getName();
  public static final String GENERATION_MANAGER_KEY = KEY_PREFIX + ".GENERATION_MANAGER";

  private KMeansGenerationManager generationManager;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    ServletContext context = config.getServletContext();
    generationManager = (KMeansGenerationManager) context.getAttribute(GENERATION_MANAGER_KEY);
  }

  final KMeansGenerationManager getGenerationManager() {
    return generationManager;
  }
}
