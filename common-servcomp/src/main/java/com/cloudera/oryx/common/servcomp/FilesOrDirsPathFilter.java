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

package com.cloudera.oryx.common.servcomp;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

final class FilesOrDirsPathFilter implements PathFilter {

  private final FileSystem fs;
  private final boolean files;

  FilesOrDirsPathFilter(FileSystem fs, boolean files) {
    this.fs = fs;
    this.files = files;
  }

  @Override
  public boolean accept(Path maybeListPath) {
    try {
      String name = maybeListPath.getName();
      return !name.endsWith("_SUCCESS") && !name.startsWith(".") && fs.isFile(maybeListPath) == files;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
