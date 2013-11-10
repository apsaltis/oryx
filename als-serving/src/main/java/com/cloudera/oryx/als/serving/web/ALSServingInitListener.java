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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.http.HttpServlet;

import org.apache.catalina.Context;
import org.apache.catalina.Wrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.OryxRecommender;
import com.cloudera.oryx.als.serving.ServerRecommender;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.serving.web.AbstractOryxServingInitListener;

/**
 * <p>This servlet lifecycle listener makes sure that the shared {@link OryxRecommender} instance
 * is initialized at startup, along with related objects, and shut down when the container is destroyed.</p>
 *
 * @author Sean Owen
 */
public final class ALSServingInitListener extends AbstractOryxServingInitListener {

  private static final Logger log = LoggerFactory.getLogger(ALSServingInitListener.class);

  @Override
  public void contextInitialized(ServletContextEvent event) {
    super.contextInitialized(event);
    ServletContext context = event.getServletContext();
    File localInputDir = getLocalInputDir();
    ServerRecommender recommender;
    try {
      recommender = new ServerRecommender(localInputDir);
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
    context.setAttribute(AbstractALSServlet.RECOMMENDER_KEY, recommender);
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {
    ServletContext context = event.getServletContext();
    Closeable recommender = (Closeable) context.getAttribute(AbstractALSServlet.RECOMMENDER_KEY);
    if (recommender != null) {
      try {
        recommender.close();
      } catch (IOException e) {
        log.warn("Unexpected error while closing", e);
      }
    }
  }

  @Override
  public HttpServlet getApiJSPXFile() {
    return new test_002dals_jspx();
  }

  @Override
  public void addServlets(Context context) {
    addServlet(context, new RecommendServlet(), "/recommend/*");
    addServlet(context, new RecommendToManyServlet(), "/recommendToMany/*");
    addServlet(context, new RecommendToAnonymousServlet(), "/recommendToAnonymous/*");
    addServlet(context, new SimilarityServlet(), "/similarity/*");
    addServlet(context, new SimilarityToItemServlet(), "/similarityToItem/*");
    addServlet(context, new EstimateServlet(), "/estimate/*");
    addServlet(context, new EstimateForAnonymousServlet(), "/estimateForAnonymous/*");
    addServlet(context, new BecauseServlet(), "/because/*");
    addServlet(context, new ReadyServlet(), "/ready/*");
    addServlet(context, new MostPopularItemsServlet(), "/mostPopularItems/*");
    if (!ConfigUtils.getDefaultConfig().getBoolean("serving-layer.api.read-only")) {
      addServlet(context, new PreferenceServlet(), "/pref/*");
      Wrapper ingestWrapper = addServlet(context, new IngestServlet(), "/ingest/*");
      ingestWrapper.setMultipartConfigElement(new MultipartConfigElement("/tmp"));
      addServlet(context, new RefreshServlet(), "/refresh/*");
    }
  }

}
