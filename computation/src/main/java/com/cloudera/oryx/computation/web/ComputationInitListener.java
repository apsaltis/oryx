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

package com.cloudera.oryx.computation.web;

import java.util.logging.Handler;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.cloudera.oryx.computation.PeriodicRunner;
import com.cloudera.oryx.common.log.MemoryHandler;
import com.cloudera.oryx.common.servcomp.web.LogServlet;

/**
 * <p>This servlet lifecycle listener makes sure that the shared resources are
 * initialized at startup, along with related objects, and shut down when the container is destroyed.</p>
 *
 * @author Sean Owen
 */
public final class ComputationInitListener implements ServletContextListener {

  public static final String PERIODIC_RUNNER_KEY = "PERIODIC_RUNNER";

  private PeriodicRunner runner;

  @Override
  public void contextInitialized(ServletContextEvent event) {
    ServletContext context = event.getServletContext();
    configureLogging(context);
    runner = new PeriodicRunner();
    context.setAttribute(PERIODIC_RUNNER_KEY, runner);
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

  @Override
  public void contextDestroyed(ServletContextEvent servletContextEvent) {
    if (runner != null) {
      runner.close();
    }
  }

}
