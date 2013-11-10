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

import java.io.IOException;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.Maps;

import com.cloudera.oryx.serving.stats.ServletStats;

/**
 * Superclass of {@link HttpServlet}s used in the application. All API methods return the following 
 * HTTP statuses in certain situations:
 *
 * <ul>
 *  <li>{@code 400 Bad Request} if the arguments are invalid</li>
 *  <li>{@code 401 Unauthorized} if a username/password is required, but not supplied correctly 
 *  in the request via HTTP DIGEST</li>
 *  <li>{@code 405 Method Not Allowed} if an incorrect HTTP method is used, like {@code GET} 
 *  where {@code POST} is required</li>
 *  <li>{@code 500 Internal Server Error} if an unexpected server-side exception occurs</li>
 *  <li>{@code 503 Service Unavailable} if not yet available to serve requests</li>
 * </ul>
 *
 * @author Sean Owen
 */
public abstract class AbstractOryxServlet extends HttpServlet {

  private static final String KEY_PREFIX = AbstractOryxServlet.class.getName();
  public static final String TIMINGS_KEY = KEY_PREFIX + ".TIMINGS";

  private ServletStats timing;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    ServletContext context = config.getServletContext();

    Map<String,ServletStats> timings;
    synchronized (context) {
      @SuppressWarnings("unchecked")
      Map<String,ServletStats> temp = (Map<String,ServletStats>) context.getAttribute(TIMINGS_KEY);
      timings = temp;
      if (timings == null) {
        timings = Maps.newTreeMap();
        context.setAttribute(TIMINGS_KEY, timings);
      }
    }

    String key = getClass().getSimpleName();
    ServletStats theTiming = timings.get(key);
    if (theTiming == null) {
      theTiming = new ServletStats();
      timings.put(key, theTiming);
    }
    timing = theTiming;
  }

  @Override
  protected final void service(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    long start = System.nanoTime();
    super.service(request, response);
    timing.addTimingNanosec(System.nanoTime() - start);

    int status = response.getStatus();
    if (status >= 400) {
      if (status >= 500) {
        timing.incrementServerErrors();
      } else {
        timing.incrementClientErrors();
      }
    }
  }

}
