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

package com.cloudera.oryx.common.iterator;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;

import com.google.common.collect.AbstractIterator;

import com.cloudera.oryx.common.io.IOUtils;

/**
 * Iterates over the lines of a text file. This assumes the text file's lines are delimited in a manner
 * consistent with how {@link BufferedReader} defines lines.
 * 
 * This class will uncompress files that end in .zip or .gz accordingly, too.
 * 
 * @author Sean Owen
 */
final class FileLineIterator extends AbstractIterator<String> implements Closeable {

  private final BufferedReader reader;
  
  FileLineIterator(Reader reader) {
    this.reader = IOUtils.buffer(reader);
  }

  @Override
  protected String computeNext() {
    String line;
    try {
      line = reader.readLine();
    } catch (IOException ioe) {
      try {
        close();
      } catch (IOException ioe2) {
        throw new IllegalStateException(ioe2);
      }
      throw new IllegalStateException(ioe);
    }
    return line == null ? endOfData() : line;
  }

  @Override
  public void close() throws IOException {
    endOfData();
    reader.close();
  }
  
}
