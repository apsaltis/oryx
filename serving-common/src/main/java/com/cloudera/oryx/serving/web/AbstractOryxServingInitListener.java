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

package com.cloudera.oryx.serving.web;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.catalina.Context;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.http.HttpServlet;
import java.io.File;
import java.io.IOException;
import java.util.logging.Handler;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.log.MemoryHandler;
import com.cloudera.oryx.common.servcomp.web.LogServlet;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Superclass of init listeners for Serving Layer APIs. Subclasses are where algorithm-specific initialization
 * for servlets for a particular type of API occurs.
 */
public abstract class AbstractOryxServingInitListener implements ServletContextListener {

  private static final Logger log = LoggerFactory.getLogger(AbstractOryxServingInitListener.class);

  public static final String READ_ONLY_KEY = "READ_ONLY";
  private static final String LOCAL_INPUT_DIR_KEY = "LOCAL_INPUT_DIR";

  private File localInputDir;

  @Override
  public void contextInitialized(ServletContextEvent event) {
    ServletContext context = event.getServletContext();
    configureLogging(context);
    configureTempDir(context);
    configureLocalInputDir();
    Config config = ConfigUtils.getDefaultConfig();
    context.setAttribute(READ_ONLY_KEY, config.getBoolean("serving-layer.api.read-only"));
    context.setAttribute(LOCAL_INPUT_DIR_KEY, getLocalInputDir());
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {
    // No-op default
  }

  protected final File getLocalInputDir() {
    return localInputDir;
  }

  private static void configureLogging(ServletContext context) {
    MemoryHandler.setSensibleLogFormat();
    Handler logHandler = null;
    for (Handler handler : java.util.logging.Logger.getLogger("").getHandlers()) {
      if (handler instanceof MemoryHandler) {
        logHandler = handler;
        break;
      }
    }
    if (logHandler == null) {
      // Not previously configured by command line, make a new one
      logHandler = new MemoryHandler();
      java.util.logging.Logger.getLogger("").addHandler(logHandler);
    }
    context.setAttribute(LogServlet.LOG_HANDLER, logHandler);
  }

  /**
   * This is a possible workaround for Tomcat on Windows, not creating the temp dir it allocates?
   */
  private static void configureTempDir(ServletContext context) {
    File tempDir = (File) context.getAttribute(ServletContext.TEMPDIR);
    Preconditions.checkNotNull(tempDir, "Servlet container didn't set %s", ServletContext.TEMPDIR);
    Preconditions.checkArgument(!tempDir.isFile());
    try {
      IOUtils.mkdirs(tempDir);
    } catch (IOException e) {
      log.warn("Can't make {}", tempDir);
    }
  }

  private void configureLocalInputDir() {
    Config config = ConfigUtils.getDefaultConfig();
    String localInputDirName = config.getString("serving-layer.local-input-dir");
    localInputDir = new File(localInputDirName);
    try {
      IOUtils.mkdirs(localInputDir);
    } catch (IOException e) {
      log.warn("Can't make {}", localInputDir);
    }
  }

  public static Wrapper addServlet(Context context, Servlet servlet, String path) {
    String name = servlet.getClass().getSimpleName();
    Wrapper servletWrapper = Tomcat.addServlet(context, name, servlet);
    servletWrapper.setLoadOnStartup(1);
    context.addServletMapping(path, name);
    return servletWrapper;
  }

  public abstract HttpServlet getApiJSPXFile();

  public abstract void addServlets(Context context);

}
