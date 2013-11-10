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

package com.cloudera.oryx.serving;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;

import com.cloudera.oryx.als.serving.web.ALSServingInitListener;
import com.cloudera.oryx.common.ClassUtils;
import com.cloudera.oryx.common.servcomp.web.LogServlet;
import com.cloudera.oryx.common.servcomp.web.style_jspx;
import com.cloudera.oryx.common.settings.APISettings;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.log.MemoryHandler;
import com.cloudera.oryx.common.signal.SignalManager;
import com.cloudera.oryx.common.signal.SignalType;
import com.cloudera.oryx.common.servcomp.web.InMemoryRealm;
import com.cloudera.oryx.kmeans.serving.web.KMeansServingInitListener;
import com.cloudera.oryx.rdf.serving.web.RDFServingInitListener;
import com.cloudera.oryx.serving.web.AbstractOryxServingInitListener;
import com.cloudera.oryx.serving.web.ConfigServlet;
import com.cloudera.oryx.serving.web.error_jspx;
import com.cloudera.oryx.serving.web.index_jspx;
import com.cloudera.oryx.serving.web.status_jspx;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleEvent;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Server;
import org.apache.catalina.authenticator.DigestAuthenticator;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.JasperListener;
import org.apache.catalina.core.JreMemoryLeakPreventionListener;
import org.apache.catalina.core.ThreadLocalLeakPreventionListener;
import org.apache.catalina.deploy.ApplicationListener;
import org.apache.catalina.deploy.ErrorPage;
import org.apache.catalina.deploy.LoginConfig;
import org.apache.catalina.deploy.SecurityCollection;
import org.apache.catalina.deploy.SecurityConstraint;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>This is the runnable class which starts the Serving Layer and its Tomcat-based HTTP server. It is
 * started with {@link #call()} and can be shut down with {@link #close()}. This implementation is used
 * both in stand-alone local mode, and in a distributed mode cooperating with a Computation Layer.</p>
 *
 * <p>This program instantiates a Tomcat-based HTTP server exposing a REST-style API. It is available via
 * HTTP, or HTTPS as well if "serving-layer.api.keystore-file" is set. It can also be password
 * protected by setting "serving-layer.api.user-name" and
 * "serving-layer.api.password".</p>
 *
 * <p>When run in local mode, the Serving Layer instance will compute a model locally and save it as files
 * under a {@code model/} directory. It will be updated when the model is rebuilt.
 * If the file is present at startup, it will be read to restore the server state, rather than re-reading
 * CSV input in the directory and recomputing the model. Thus the file can be saved and restored as a
 * way of preserving and recalling the server's state of learning.</p>
 *
 * <p>Example of running in local mode:</p>
 *
 * <p>{@code java -jar oryx-serving-x.y.jar}</p>
 *
 * <p>(with an example of JVM tuning flags:)</p>
 *
 * <p>{@code java -Xmx1g -XX:NewRatio=12 -XX:+UseParallelOldGC -jar oryx-serving-x.y.jar}</p>
 *
 * <p>Finally, some more advanced tuning parameters are available. These are system parameters, set with
 * {@code -Dproperty=value}.</p>
 *
 * @author Sean Owen
 */
public final class Runner implements Callable<Boolean>, Closeable {

  private static final Logger log = LoggerFactory.getLogger(Runner.class);

  private static final Map<String,Class<? extends AbstractOryxServingInitListener>> LISTENERS =
      ImmutableMap.of(
          "als", ALSServingInitListener.class,
          "kmeans", KMeansServingInitListener.class,
          "rdf", RDFServingInitListener.class
      );

  private static final int[] ERROR_PAGE_STATUSES = {
      HttpServletResponse.SC_BAD_REQUEST,
      HttpServletResponse.SC_UNAUTHORIZED,
      HttpServletResponse.SC_NOT_FOUND,
      HttpServletResponse.SC_METHOD_NOT_ALLOWED,
      HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
      HttpServletResponse.SC_NOT_IMPLEMENTED,      
      HttpServletResponse.SC_SERVICE_UNAVAILABLE,
  };

  private final Config config;
  private Tomcat tomcat;
  private final File noSuchBaseDir;
  private boolean closed;

  /**
   * Creates a new instance with the given configuration.
   */
  public Runner() {
    this.config = ConfigUtils.getDefaultConfig();
    this.noSuchBaseDir = Files.createTempDir();
    this.noSuchBaseDir.deleteOnExit();
  }

  public static void main(String[] args) throws Exception {

    final Runner runner = new Runner();
    runner.call();

    SignalManager.register(new Runnable() {
        @Override
        public void run() {
          runner.close();
        }
      }, SignalType.INT, SignalType.TERM);

    runner.await();
    runner.close();
  }

  @Override
  public Boolean call() throws IOException {

    MemoryHandler.setSensibleLogFormat();
    java.util.logging.Logger.getLogger("").addHandler(new MemoryHandler());

    Tomcat tomcat = new Tomcat();
    Connector connector = makeConnector();
    configureTomcat(tomcat, connector);
    configureEngine(tomcat.getEngine());
    configureServer(tomcat.getServer());
    configureHost(tomcat.getHost());
    Context context = makeContext(tomcat, noSuchBaseDir);

    AbstractOryxServingInitListener apiListener = chooseAPIListener();
    context.addApplicationListener(new ApplicationListener(apiListener.getClass().getName(), false));

    AbstractOryxServingInitListener.addServlet(context, new style_jspx(), "/style.jspx");
    AbstractOryxServingInitListener.addServlet(context, new index_jspx(), "/index.jspx");
    AbstractOryxServingInitListener.addServlet(context, new status_jspx(), "/status.jspx");
    AbstractOryxServingInitListener.addServlet(context, new error_jspx(), "/error.jspx");
    AbstractOryxServingInitListener.addServlet(context, new LogServlet(), "/log.txt");
    AbstractOryxServingInitListener.addServlet(context, new ConfigServlet(), "/config.json");
    AbstractOryxServingInitListener.addServlet(context, apiListener.getApiJSPXFile(), "/test.jspx");
    apiListener.addServlets(context);

    try {
      tomcat.start();
    } catch (LifecycleException le) {
      throw new IOException(le);
    }
    this.tomcat = tomcat;
    return Boolean.TRUE;
  }

  /**
   * Blocks and waits until the server shuts down.
   */
  public void await() {
    tomcat.getServer().await();
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      if (tomcat != null) {
        try {
          tomcat.stop();
          tomcat.destroy();
        } catch (LifecycleException le) {
          log.warn("Unexpected error while stopping", le);
        }
        try {
          IOUtils.deleteRecursively(noSuchBaseDir);
        } catch (IOException e) {
          log.warn("Failed to delete {}", noSuchBaseDir);
        }
      }
    }
  }

  private void configureTomcat(Tomcat tomcat, Connector connector) {
    tomcat.setBaseDir(noSuchBaseDir.getAbsolutePath());
    tomcat.setConnector(connector);
    tomcat.getService().addConnector(connector);
  }

  private void configureEngine(Engine engine) {
    APISettings apiSettings = APISettings.create(config.getConfig("serving-layer.api"));
    String userName = apiSettings.getUserName();
    String password = apiSettings.getPassword();
    if (userName != null && password != null) {
      InMemoryRealm realm = new InMemoryRealm();
      realm.addUser(userName, password);
      engine.setRealm(realm);
    }
  }

  private static void configureServer(Server server) {
    LifecycleListener jasperListener = new JasperListener();
    server.addLifecycleListener(jasperListener);
    jasperListener.lifecycleEvent(new LifecycleEvent(server, Lifecycle.BEFORE_INIT_EVENT, null));
    server.addLifecycleListener(new JreMemoryLeakPreventionListener());
    server.addLifecycleListener(new ThreadLocalLeakPreventionListener());
  }

  private static void configureHost(Host host) {
    host.setAutoDeploy(false);
  }

  private Connector makeConnector() {
    Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
    APISettings apiSettings = APISettings.create(config.getConfig("serving-layer.api"));

    File keystoreFile = apiSettings.getKeystoreFile();
    String keystorePassword = apiSettings.getKeystorePassword();
    if (keystoreFile == null && keystorePassword == null) {
      // HTTP connector
      connector.setPort(apiSettings.getPort());
      connector.setSecure(false);
      connector.setScheme("http");

    } else {

      // HTTPS connector
      connector.setPort(apiSettings.getSecurePort());
      connector.setSecure(true);
      connector.setScheme("https");
      connector.setAttribute("SSLEnabled", "true");
      String protocol = chooseSSLProtocol("TLSv1.1", "TLSv1");
      if (protocol != null) {
        connector.setAttribute("sslProtocol", protocol);
      }
      if (keystoreFile != null) {
        connector.setAttribute("keystoreFile", keystoreFile.getAbsoluteFile());
      }
      connector.setAttribute("keystorePass", keystorePassword);
    }

    // Keep quiet about the server type
    connector.setXpoweredBy(false);
    connector.setAttribute("server", "Oryx");

    // Basic tuning params:
    connector.setAttribute("maxThreads", 400);
    connector.setAttribute("acceptCount", 50);
    //connector.setAttribute("connectionTimeout", 2000);
    connector.setAttribute("maxKeepAliveRequests", 100);

    // Avoid running out of ephemeral ports under heavy load?
    connector.setAttribute("socket.soReuseAddress", true);
    
    connector.setMaxPostSize(0);
    connector.setAttribute("disableUploadTimeout", false);

    return connector;
  }
  
  private static String chooseSSLProtocol(String... protocols) {
    for (String protocol : protocols) {
      try {
        SSLContext.getInstance(protocol);
        return protocol;
      } catch (NoSuchAlgorithmException ignored) {
        // continue
      }
    }
    return null;
  }

  private Context makeContext(Tomcat tomcat, File noSuchBaseDir) throws IOException {

    File contextPath = new File(noSuchBaseDir, "context");
    IOUtils.mkdirs(contextPath);

    APISettings apiSettings = APISettings.create(config.getConfig("serving-layer.api"));
    String contextPathURIBase = config.getString("serving-layer.api.context-path");
    if (contextPathURIBase == null || contextPathURIBase.isEmpty() || "/".equals(contextPathURIBase)) {
      contextPathURIBase = "";
    }
    Context context = tomcat.addContext(contextPathURIBase, contextPath.getAbsolutePath());

    context.setWebappVersion("3.0");
    context.addWelcomeFile("index.jspx");
    addErrorPages(context);

    boolean needHTTPS = apiSettings.isSecure();
    boolean needAuthentication = apiSettings.getUserName() != null;

    if (needHTTPS || needAuthentication) {

      SecurityCollection securityCollection = new SecurityCollection("Protected Resources");
      securityCollection.addPattern("/*");
      SecurityConstraint securityConstraint = new SecurityConstraint();
      securityConstraint.addCollection(securityCollection);

      if (needHTTPS) {
        securityConstraint.setUserConstraint("CONFIDENTIAL");
      }

      if (needAuthentication) {

        LoginConfig loginConfig = new LoginConfig();
        loginConfig.setAuthMethod("DIGEST");
        loginConfig.setRealmName(InMemoryRealm.NAME);
        context.setLoginConfig(loginConfig);

        securityConstraint.addAuthRole(InMemoryRealm.AUTH_ROLE);

        context.addSecurityRole(InMemoryRealm.AUTH_ROLE);
        DigestAuthenticator authenticator = new DigestAuthenticator();
        authenticator.setNonceValidity(10 * 1000L); // Shorten from 5 minutes to 10 seconds
        authenticator.setNonceCacheSize(20000); // Increase from 1000 to 20000
        context.getPipeline().addValve(authenticator);
      }

      context.addConstraint(securityConstraint);
    }

    context.setCookies(false);

    return context;
  }

  private AbstractOryxServingInitListener chooseAPIListener() {
    Class<? extends AbstractOryxServingInitListener> listenerClass = LISTENERS.get(config.getString("model.type"));
    Preconditions.checkNotNull(listenerClass,
                               "Unspecified or unsupported model type: %s",
                               config.getString("model.type"));
    return ClassUtils.loadInstanceOf(listenerClass);
  }

  private static void addErrorPages(Context context) {
    for (int errorCode : ERROR_PAGE_STATUSES) {
      ErrorPage errorPage = new ErrorPage();
      errorPage.setErrorCode(errorCode);
      errorPage.setLocation("/error.jspx");
      context.addErrorPage(errorPage);
    }
    ErrorPage errorPage = new ErrorPage();
    errorPage.setExceptionType(Throwable.class.getName());
    errorPage.setLocation("/error.jspx");
    context.addErrorPage(errorPage);
  }

}
