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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import com.cloudera.oryx.common.io.IOUtils;

/**
 * Iterable representing the lines of a text file. It can produce an {@link Iterator} over those lines. This
 * assumes the text file's lines are delimited in a manner consistent with how {@link java.io.BufferedReader}
 * defines lines.
 * 
 * This class will uncompress files that end in .zip or .gz accordingly, too.
 * 
 * @author Sean Owen
 */
public final class FileLineIterable implements Iterable<String> {

  private final Reader reader;

  /** 
   * Creates a {@code FileLineIterable} over a given {@link File}, assuming a UTF-8 encoding.
   */
  public FileLineIterable(File file) throws IOException {
    this.reader = IOUtils.openReaderMaybeDecompressing(file);
  }

  /**
   * Creates a {@code FileLineIterable} over a given stream of characters from a {@link Reader},
   * assuming a UTF-8 encoding.
   */
  public FileLineIterable(Reader reader) {
    this.reader = reader;
  }
  
  @Override
  public Iterator<String> iterator() {
    return new FileLineIterator(reader);
  }
  
}