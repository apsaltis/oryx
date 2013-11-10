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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Interface to backing store -- for now, HDFS. This allows the Hadoop-compatible
 * binaries to write most of their code in terms of abstract operations.
 *
 * @author Sean Owen
 */
public final class Store {

  private static final Logger log = LoggerFactory.getLogger(Store.class);

  private static final Store INSTANCE = new Store();

  private final FileSystem fs;

  private Store() {
    try {
      Configuration conf = new OryxConfiguration();
      if (ConfigUtils.getDefaultConfig().getBoolean("model.local")) {
        fs = FileSystem.getLocal(conf);
      } else {
        fs = FileSystem.get(URI.create(Namespaces.get().getPrefix()), conf);
      }
    } catch (IOException ioe) {
      log.error("Unable to configure Store", ioe);
      throw new IllegalStateException(ioe);
    }
  }

  /**
   * @return singleton {@code Store} instance
   */
  public static Store get() {
    return INSTANCE;
  }

  /**
   * Detects if a file or directory exists in the remote file system.
   *
   * @param key file to test
   * @param isFile if true, test for a file, otherwise for a directory
   * @return {@code true} iff the file exists
   */
  public boolean exists(String key, boolean isFile) throws IOException {
    Preconditions.checkNotNull(key);
    Path path = Namespaces.toPath(key);
    return fs.exists(path) && (fs.isFile(path) == isFile);
  }

  /**
   * Gets the size in bytes of a remote file.
   *
   * @param key file to test
   * @return size of file in bytes
   * @throws java.io.FileNotFoundException if there is no file at the key
   */
  public long getSize(String key) throws IOException {
    Preconditions.checkNotNull(key);
    Path path = Namespaces.toPath(key);
    return fs.getFileStatus(path).getLen();
  }

  /**
   * Gets the total size of all files in all subdirectories of a path.
   *
   * @param key path to compute size of
   * @return total number of bytes at the requested path in bytes
   */
  public long getSizeRecursive(String key) throws IOException {
    // Impl based on FileUtil.getDU()
    Preconditions.checkNotNull(key);
    Path path = Namespaces.toPath(key);

    if (!fs.exists(path)) {
      return 0L;
    }
    if (!fs.isDirectory(path)) {
      return fs.getFileStatus(path).getLen();
    }

    // path is a directory

    RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, true);
    long size = 0L;
    while (it.hasNext()) {
      FileStatus f = it.next();
      Path p = f.getPath();
      if (fs.isDirectory(p)) {
        size += getSizeRecursive(p.toString());
      } else {
        size += f.getLen();
      }
    }

    return size;
  }

  /**
   * @param key file to read
   * @return a byte stream delivering the file's contents
   * @throws IOException if an error occurs, like the file doesn't exist
   */
  public InputStream streamFrom(String key) throws IOException {
    Preconditions.checkNotNull(key);
    InputStream in = fs.open(Namespaces.toPath(key));
    if (key.endsWith(".gz")) {
      in = new GZIPInputStream(in);
    } else if (key.endsWith(".zip")) {
      in = new ZipInputStream(in);
    }
    return in;
  }

  /**
   * @param key file to write
   * @return A byte stream to send data to
   * @throws IOException if an error occurs, or if the file already exists
   */
  public OutputStream streamTo(String key) throws IOException {
    return fs.create(Namespaces.toPath(key));
  }

  /**
   * @param key text file to read
   * @return a character stream delivering the file's contents
   * @throws IOException if an error occurs, like the file doesn't exist
   */
  public BufferedReader readFrom(String key) throws IOException {
    return new BufferedReader(new InputStreamReader(streamFrom(key), Charsets.UTF_8), 1 << 20); // ~1MB
  }

  private void makeParentDirs(Path path) throws IOException {
    Preconditions.checkNotNull(path);
    Path parent = path.getParent();
    boolean success;
    try {
      success = fs.mkdirs(parent);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", parent);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't make " + parent);
    }
  }

  /**
   * @param key location of file to download from distibuted storage
   * @param file local {@link File} to store data into -- can be directory or explicit file
   * @throws IOException if an error occurs while downloading
   */
  public void download(String key, File file) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(file);
    Path path = Namespaces.toPath(key);
    Path filePath = new Path(file.getAbsolutePath());
    fs.copyToLocalFile(false, path, filePath);
  }

  /**
   * @param dirKey location of directory whose contents will be downloaded
   * @param dir local {@link File} to store files/directories under
   * @throws IOException if an error occurs while downloading
   */
  public void downloadDirectory(String dirKey, File dir) throws IOException {
    Preconditions.checkNotNull(dirKey);
    Preconditions.checkNotNull(dir);
    Preconditions.checkArgument(dir.exists() && dir.isDirectory(), "Not a directory: %s", dir);

    Path dirPath = Namespaces.toPath(dirKey);
    if (!fs.exists(dirPath)) {
      return;
    }
    Preconditions.checkArgument(fs.getFileStatus(dirPath).isDirectory(), "Not a directory: %s", dirPath);

    boolean complete = false;
    try {
      for (FileStatus status : fs.listStatus(dirPath)) {
        String name = status.getPath().getName();
        String fromKey = dirKey + '/' + name;
        File toLocal = new File(dir, name);
        if (status.isFile()) {
          download(fromKey, toLocal);
        } else {
          if (!toLocal.mkdir()) {
            throw new IOException("Can't make " + toLocal);
          }
          downloadDirectory(fromKey, toLocal);
        }
      }
      complete = true;
    } finally {
      if (!complete) {
        log.warn("Failed to download {} so deleting {}", dirKey, dir);
        IOUtils.deleteRecursively(dir);
      }
    }
  }

  /**
   * Uploads a local file to a remote file.
   *
   * @param key file to write to
   * @param file file bytes to upload
   * @param overwrite if true, overwrite the existing file data if exists already
   * @throws IOException if the data can't be written, or file exists and overwrite is false
   */
  public void upload(String key, File file, boolean overwrite) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(file);
    Preconditions.checkArgument(file.exists(), "Doesn't exist: %s", file);
    Preconditions.checkArgument(file.isFile(), "Not a file: %s", file);
    if (!overwrite && exists(key, true)) {
      throw new IOException(key + " already exists");
    }

    Path path = Namespaces.toPath(key);
    makeParentDirs(path);
    Path filePath = new Path(file.getAbsolutePath());
    try {
      fs.copyFromLocalFile(false, overwrite, filePath, path);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!fs.exists(path)) {
      throw new IOException("Couldn't upload " + filePath + " to " + path);
    }
  }

  /**
   * Uploads a directory recurisvely
   *
   * @param dirKey location under which to store the contents found under {@code dir}
   * @param dir directory whose <em>contents</em> will be uploaded
   * @param overwrite if true, overwrite existing files
   * @throws IOException if the data can't be written, or file exists and overwrite is false
   */
  public void uploadDirectory(String dirKey, File dir, boolean overwrite) throws IOException {
    Preconditions.checkNotNull(dirKey);
    Preconditions.checkNotNull(dir);
    Preconditions.checkArgument(dir.exists() && dir.isDirectory(), "Not a directory: %s", dir);
    File[] contents = dir.listFiles(IOUtils.NOT_HIDDEN);
    if (contents != null) {
      boolean complete = false;
      try {
        for (File content : contents) {
          String toKey = dirKey + '/' + content.getName();
          if (content.isFile()) {
            upload(toKey, content, overwrite);
          } else {
            uploadDirectory(toKey, content, overwrite);
          }
        }
        complete = true;
      } finally {
        if (!complete) {
          log.warn("Failed to upload {} so deleting {}", dir, dirKey);
          recursiveDelete(dirKey);
        }
      }
    }
  }

  /**
   * Creates a 0-length file.
   *
   * @param key file to create
   */
  public void touch(String key) throws IOException {
    Preconditions.checkNotNull(key);
    Path path = Namespaces.toPath(key);
    makeParentDirs(path);
    boolean success;
    try {
      success = fs.createNewFile(path);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't create " + path);
    }
  }

  /**
   * Makes a directory. If the file system doesn't have an idea of directories, it makes a 0-length file.
   *
   * @param key directory to create
   */
  public void mkdir(String key) throws IOException {
    Preconditions.checkNotNull(key);
    Path path = Namespaces.toPath(key);
    boolean success;
    try {
      success = fs.mkdirs(path);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't mkdirs for " + path);
    }
  }

  /**
   * Deletes the file at the given location.
   *
   * @param key file to delete
   */
  public void delete(String key) throws IOException {
    Preconditions.checkNotNull(key);
    Path path = Namespaces.toPath(key);
    if (!fs.isFile(path)) {
      throw new IOException("Not a file: " + path);
    }
    boolean success;
    try {
      success = fs.delete(path, false);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't delete " + path);
    }
  }

  /**
   * Recursively deletes a file/directory. If the file system does not have a notion of directories, this deletes
   * all keys that begin with the given prefix.
   *
   * @param keyPrefix file/directory ("prefix") to delete
   */
  public void recursiveDelete(String keyPrefix) throws IOException {
    Preconditions.checkNotNull(keyPrefix);
    Path path = Namespaces.toPath(keyPrefix);
    if (!fs.exists(path)) {
      return;
    }

    boolean success;
    try {
      log.info("Deleting recursively: {}", path);
      success = fs.delete(path, true);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't delete " + path);
    }
  }

  /**
   * Lists contents of a directory. For file systems without a notion of directory, this lists prefixes that
   * have the same prefix as the given prefix, but excludes "directories" (keys with same prefix, but followed
   * by more path elements). Results are returned in lexicographically sorted order.
   *
   * @param prefix directory to list
   * @param files if true, only list files, not directories
   * @return list of keys representing directory contents
   */
  public List<String> list(String prefix, boolean files) throws IOException {
    Preconditions.checkNotNull(prefix);
    Path path = Namespaces.toPath(prefix);
    if (!fs.exists(path)) {
      return Collections.emptyList();
    }

    Preconditions.checkArgument(fs.getFileStatus(path).isDirectory(), "Not a directory: %s", path);
    FileStatus[] statuses = fs.listStatus(path, new FilesOrDirsPathFilter(fs, files));
    String prefixString = Namespaces.get().getPrefix();

    List<String> result = Lists.newArrayListWithCapacity(statuses.length);
    for (FileStatus fileStatus : statuses) {
      String listPath = fileStatus.getPath().toString();
      Preconditions.checkState(listPath.startsWith(prefixString),
                               "%s doesn't start with %s", listPath, prefixString);
      if (!listPath.endsWith("_SUCCESS")) {
        listPath = listPath.substring(prefixString.length());
        if (fileStatus.isDirectory() && !listPath.endsWith("/")) {
          listPath += "/";
        }
        result.add(listPath);
      }
    }
    Collections.sort(result);
    return result;
  }

  /**
   * @param key file to test
   * @return last-modified time of file, in milliseconds since the epoch
   * @throws java.io.FileNotFoundException if the file does not exist
   */
  public long getLastModified(String key) throws IOException {
    Preconditions.checkNotNull(key);
    Path path = Namespaces.toPath(key);
    FileStatus status = fs.getFileStatus(path);
    return status.getModificationTime();
  }

}
