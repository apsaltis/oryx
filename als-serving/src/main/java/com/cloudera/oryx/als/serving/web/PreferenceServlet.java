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

package com.cloudera.oryx.als.serving.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cloudera.oryx.common.LangUtils;
import com.cloudera.oryx.als.common.OryxRecommender;

/**
 * <p>Responds to a POST request to {@code /pref/[userID]/[itemID]} and in turn calls
 * {@link OryxRecommender#setPreference(String, String, float)}. If the request body is empty,
 * the value is 1.0, otherwise the value in the request body's first line is used.</p>
 *
 * <p>Also responds to a DELETE request to the same path, with the same defaults. This corresponds
 * to calling {@link OryxRecommender#removePreference(String, String)} instead.</p>
 *
 * @author Sean Owen
 */
public final class PreferenceServlet extends AbstractALSServlet {

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    String userID;
    String itemID;
    try {
      userID = pathComponents.next();
      itemID = pathComponents.next();
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }
    if (pathComponents.hasNext()) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Path too long");
      return;
    }

    float prefValue;
    try {
      prefValue = readValue(request);
    } catch (IllegalArgumentException ignored) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad value");
      return;
    }

    OryxRecommender recommender = getRecommender();
    recommender.setPreference(userID, itemID, prefValue);
  }

  @Override
  protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws IOException {
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    String userID;
    String itemID;
    try {
      userID = pathComponents.next();
      itemID = pathComponents.next();
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }

    OryxRecommender recommender = getRecommender();
    recommender.removePreference(userID, itemID);
  }

  private static float readValue(ServletRequest request) throws IOException {
    String line;
    BufferedReader reader = request.getReader();
    try {
      line = reader.readLine();
    } finally {
      reader.close();
    }
    if (line == null || line.isEmpty()) {
      return 1.0f;
    }
    return LangUtils.parseFloat(line);
  }

}
