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

import com.cloudera.oryx.kmeans.common.Centers;
import com.cloudera.oryx.kmeans.common.Distance;
import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.RealVector;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.kmeans.serving.generation.Generation;

/**
 * <p>Responsds to a GET request to {@code /distanceToNearest/[datum]}. The input is one data point to cluster,
 * delimited, like "1,-4,3.0". The response body contains the distance to the nearest cluster, on one line.</p>
 *
 * @author Sean Owen
 */
public final class DistanceToNearestServlet extends AbstractKMeansServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    String line = pathInfo.subSequence(1, pathInfo.length()).toString();

    Generation generation = getGenerationManager().getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return;
    }

    String[] tokens = DelimitedDataUtils.decode(line);
    RealVector vector = generation.toVector(tokens);
    if (vector == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
      return;
    }
    double distance = findClosest(generation, vector).getSquaredDistance();
    response.getWriter().write(Double.toString(distance));
  }

  static Distance findClosest(Generation generation, RealVector parsedVector) {
    Centers centroids = generation.getCentroids();
    Preconditions.checkState(centroids.size() > 0);
    return centroids.getDistance(parsedVector);
  }
}
