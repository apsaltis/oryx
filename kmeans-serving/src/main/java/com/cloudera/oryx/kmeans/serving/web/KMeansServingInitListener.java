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

import org.apache.catalina.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.http.HttpServlet;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kmeans.serving.generation.KMeansGenerationManager;
import com.cloudera.oryx.serving.web.AbstractOryxServingInitListener;

public final class KMeansServingInitListener extends AbstractOryxServingInitListener {

  private static final Logger log = LoggerFactory.getLogger(KMeansServingInitListener.class);

  @Override
  public void contextInitialized(ServletContextEvent event) {
    super.contextInitialized(event);
    ServletContext context = event.getServletContext();
    File localInputDir = getLocalInputDir();
    KMeansGenerationManager generationManager;
    try {
      generationManager = new KMeansGenerationManager(localInputDir);
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
    context.setAttribute(AbstractKMeansServlet.GENERATION_MANAGER_KEY, generationManager);
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {
    ServletContext context = event.getServletContext();
    Closeable manager = (Closeable) context.getAttribute(AbstractKMeansServlet.GENERATION_MANAGER_KEY);
    if (manager != null) {
      try {
        manager.close();
      } catch (IOException e) {
        log.warn("Unexpected error while closing", e);
      }
    }
  }

  @Override
  public HttpServlet getApiJSPXFile() {
    return new test_002dkmeans_jspx();
  }

  @Override
  public void addServlets(Context context) {
    addServlet(context, new AssignServlet(), "/assign/*");
    addServlet(context, new DistanceToNearestServlet(), "/distanceToNearest/*");
    if (!ConfigUtils.getDefaultConfig().getBoolean("serving-layer.api.read-only")) {
      addServlet(context, new AddServlet(), "/add/*");
      addServlet(context, new RefreshServlet(), "/refresh/*");
    }
  }

}
