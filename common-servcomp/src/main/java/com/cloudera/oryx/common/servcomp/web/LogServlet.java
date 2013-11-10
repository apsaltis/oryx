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

package com.cloudera.oryx.common.servcomp.web;

import java.io.IOException;
import java.io.Writer;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Charsets;

import com.cloudera.oryx.common.log.MemoryHandler;

/**
 * Prints recent log messages to the response.
 *
 * @author Sean Owen
 */
public final class LogServlet extends HttpServlet {

  public static final String LOG_HANDLER = LogServlet.class.getName() + ".LOG_HANDLER";

  //private static final String CONTENT_TYPE = MediaType.PLAIN_TEXT_UTF_8.withoutParameters().toString();
  private static final String CONTENT_TYPE = "text/plain; charset=UTF-8";

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    MemoryHandler logHandler = (MemoryHandler) getServletContext().getAttribute(LOG_HANDLER);
    response.setContentType(CONTENT_TYPE);
    response.setCharacterEncoding(Charsets.UTF_8.name());
    Writer out = response.getWriter();
    Iterable<String> lines = logHandler.getLogLines();
    synchronized (lines) {
      for (String line : lines) {
        out.write(line); // Already has newline
      }
    }
  }

}
