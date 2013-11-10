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

package com.cloudera.oryx.als.common.pmml;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.dmg.pmml.Extension;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import com.cloudera.oryx.common.io.IOUtils;

/**
 * A model description for an ALS model, which in practice is purely a set of paths containing actual data.
 * It uses PMML serialization but the serialization only uses a few {@code <Extension>} elements to encode
 * these paths.
 */
public final class ALSModelDescription {

  private final Map<String,String> pathByKey = Maps.newHashMap();

  private Map<String,String> getPathByKey() {
    return pathByKey;
  }

  public String getKnownItemsPath() {
    return pathByKey.get("knownItemsPath");
  }

  public void setKnownItemsPath(String path) {
    pathByKey.put("knownItemsPath", path);
  }

  public String getXPath() {
    return pathByKey.get("xPath");
  }

  public void setXPath(String path) {
    pathByKey.put("xPath", path);
  }

  public String getYPath() {
    return pathByKey.get("yPath");
  }

  public void setYPath(String path) {
    pathByKey.put("yPath", path);
  }

  public String getIDMappingPath() {
    return pathByKey.get("idMappingPath");
  }

  public void setIDMappingPath(String path) {
    pathByKey.put("idMappingPath", path);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ALSModelDescription)) {
      return false;
    }
    ALSModelDescription other = (ALSModelDescription) o;
    return pathByKey.equals(other.pathByKey);
  }

  @Override
  public int hashCode() {
    return pathByKey.hashCode();
  }

  @Override
  public String toString() {
    return pathByKey.toString();
  }

  public static ALSModelDescription read(File f) throws IOException {
    InputStream in = IOUtils.openMaybeDecompressing(f);
    try {
      return read(in);
    } catch (JAXBException jaxbe) {
      throw new IOException(jaxbe);
    } catch (SAXException saxe) {
      throw new IOException(saxe);
    } finally {
      in.close();
    }
  }

  /**
   * Quite manually parse our fake model representation in PMML.
   */
  private static ALSModelDescription read(InputStream in) throws JAXBException, SAXException {

    PMML pmml = IOUtil.unmarshal(in);
    List<Extension> extensions = pmml.getExtensions();
    Preconditions.checkNotNull(extensions);
    Preconditions.checkArgument(!extensions.isEmpty());

    ALSModelDescription model = new ALSModelDescription();

    for (Extension extension : extensions) {
      String name  = extension.getName();
      String value = extension.getValue();
      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(value);
      model.getPathByKey().put(name, value);
    }

    return model;
  }

  public static void write(File f, ALSModelDescription model) throws IOException {
    OutputStream out = IOUtils.buildGZIPOutputStream(new FileOutputStream(f));
    try {
      write(out, model);
    } catch (JAXBException jaxbe) {
      throw new IOException(jaxbe);
    } finally {
      out.close();
    }
  }

  /**
   * Quite manually write our fake model representation in PMML.
   */
  private static void write(OutputStream out, ALSModelDescription model) throws JAXBException {
    PMML pmml = new PMML(null, null, "4.1");
    for (Map.Entry<String,String> entry : model.getPathByKey().entrySet()) {
      Extension extension = new Extension();
      extension.setName(entry.getKey());
      extension.setValue(entry.getValue());
      pmml.getExtensions().add(extension);
    }
    IOUtil.marshal(pmml, out);
  }

}
