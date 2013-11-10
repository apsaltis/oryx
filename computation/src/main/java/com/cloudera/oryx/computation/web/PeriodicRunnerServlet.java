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

import java.io.IOException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.computation.PeriodicRunner;

/**
 * Manages the {@link PeriodicRunner} in the {@link ServletContext}.
 *
 * @author Sean Owen
 */
public final class PeriodicRunnerServlet extends HttpServlet {

  private static final Logger log = LoggerFactory.getLogger(PeriodicRunnerServlet.class);

  private static final String PREFIX = "/periodicRunner/";

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

    ServletContext context = getServletContext();
    PeriodicRunner runner = (PeriodicRunner) context.getAttribute(ComputationInitListener.PERIODIC_RUNNER_KEY);

    String requestURI = request.getRequestURI();
    Preconditions.checkArgument(requestURI.startsWith(PREFIX), "Bad request URI: %s", requestURI);
    String command = requestURI.substring(PREFIX.length());

    if ("forceRun".equals(command)) {
      log.info("Forcing run of PeriodicRunner");
      runner.forceRun();
    } else if ("interrupt".equals(command)) {
      log.info("Interrupting PeriodicRunner");
      runner.interrupt();
    } else {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown command");
      return;
    }

    response.sendRedirect("/index.jspx");
  }

}
