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
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.Lists;

import com.cloudera.oryx.als.common.NoSuchItemException;
import com.cloudera.oryx.als.common.NotReadyException;
import com.cloudera.oryx.als.common.OryxRecommender;

/**
 * <p>Responds to a GET request to {@code /similarityToItem/[toItemID]/itemID1(/[itemID2]/...)},
 * and in turn calls {@link OryxRecommender#similarityToItem(String, String...)} with the supplied values.</p>
 *
 * <p>Unknown item IDs are ignored, unless all are unknown or {@code toItemID} is unknown, in which case a
 * {@link HttpServletResponse#SC_BAD_REQUEST} status is returned.</p>
 *
 * <p>The output are similarities, in the same order as the item IDs, one per line.</p>
 *
 * @author Sean Owen
 */
public final class SimilarityToItemServlet extends AbstractALSServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    String toItemID;
    List<String> itemIDs = Lists.newArrayList();
    try {
      toItemID = pathComponents.next();
      while (pathComponents.hasNext()) {
        itemIDs.add(pathComponents.next());
      }
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }
    if (itemIDs.isEmpty()) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No items");
      return;
    }

    OryxRecommender recommender = getRecommender();
    try {
      float[] similarities = recommender.similarityToItem(toItemID, itemIDs.toArray(new String[itemIDs.size()]));
      Writer out = response.getWriter();
      for (float similarity : similarities) {
        out.write(Float.toString(similarity));
        out.write('\n');        
      }
    } catch (NoSuchItemException nsie) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, nsie.toString());
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    }
  }

}
