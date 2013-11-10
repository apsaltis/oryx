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

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * <p>Responds to a HEAD or GET request to {@code /ready} and in turn calls
 * {@link com.cloudera.oryx.als.common.OryxRecommender#isReady()}. Returns "OK" or "Unavailable" status depending on
 * whether the recommender is ready.</p>
 *
 * @author Sean Owen
 */
public final class ReadyServlet extends AbstractALSServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    doHead(request, response);
  }

  @Override
  protected void doHead(HttpServletRequest request, HttpServletResponse response) throws IOException {
    boolean isReady = getRecommender().isReady();
    if (isReady) {
      response.setStatus(HttpServletResponse.SC_OK);
    } else {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }
  }

}
