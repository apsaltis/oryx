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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import java.io.IOException;
import java.io.Writer;

import com.cloudera.oryx.als.common.IDValue;
import com.cloudera.oryx.als.common.OryxRecommender;
import com.cloudera.oryx.als.serving.RescorerProvider;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.serving.web.AbstractOryxServlet;

/**
 * Superclass of all servletes used in the ALS recommender API.
 *
 * @author Sean Owen
 */
public abstract class AbstractALSServlet extends AbstractOryxServlet {

  private static final int DEFAULT_HOW_MANY = 10;
  static final Splitter SLASH = Splitter.on('/').omitEmptyStrings();

  private static final String KEY_PREFIX = AbstractALSServlet.class.getName();
  public static final String RECOMMENDER_KEY = KEY_PREFIX + ".RECOMMENDER";
  private static final String RESCORER_PROVIDER_KEY = KEY_PREFIX + ".RESCORER_PROVIDER";
  private static final String[] NO_PARAMS = new String[0];

  private OryxRecommender recommender;
  private RescorerProvider rescorerProvider;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    ServletContext context = config.getServletContext();
    recommender = (OryxRecommender) context.getAttribute(RECOMMENDER_KEY);
    rescorerProvider = (RescorerProvider) context.getAttribute(RESCORER_PROVIDER_KEY);
  }

  final OryxRecommender getRecommender() {
    return recommender;
  }

  final RescorerProvider getRescorerProvider() {
    return rescorerProvider;
  }

  static int getHowMany(ServletRequest request) {
    String howManyString = request.getParameter("howMany");
    if (howManyString == null) {
      return DEFAULT_HOW_MANY;
    }
    int howMany = Integer.parseInt(howManyString);
    Preconditions.checkArgument(howMany > 0, "howMany must be positive");
    return howMany;
  }

  static String[] getRescorerParams(ServletRequest request) {
    String[] rescorerParams = request.getParameterValues("rescorerParams");
    return rescorerParams == null ? NO_PARAMS : rescorerParams;
  }

  static boolean getConsiderKnownItems(ServletRequest request) {
    return Boolean.valueOf(request.getParameter("considerKnownItems"));
  }

  /**
   * <p>CSV output contains one recommendation per line, and each line is of the form {@code itemID,strength},
   * like {@code "ABC",0.53}. Strength is an opaque indicator of the relative quality of the recommendation.</p>
   */
  final void output(ServletResponse response, Iterable<IDValue> items) throws IOException {
    Writer writer = response.getWriter();
    for (IDValue item : items) {
      writer.write(DelimitedDataUtils.encode(item.getID(), Float.toString(item.getValue())));
      writer.write('\n');
    }
  }

}
