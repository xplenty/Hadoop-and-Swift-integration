/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Swift file system implementation. Extends Hadoop FileSystem
 */
public class SwiftNativeFileSystem extends FileSystem {


  private static final Log LOG =
    LogFactory.getLog(SwiftNativeFileSystem.class);

  /**
   * URI constant for this filesystem: {@value}
   */
  public static final String SWIFT_FS = "swift";
  /**
   * path to user work directory for storing temporary files
   */
  private Path workingDir;

  /**
   * Swift URI
   */
  private URI uri;

  /**
   * reference to swiftFileSystemStore
   */
  private SwiftNativeFileSystemStore store;

  /**
   * Default constructor for Hadoop
   */
  public SwiftNativeFileSystem() {
    // set client in initialize()
  }

  /**
   * This construstor used for testing purposes
   */
  public SwiftNativeFileSystem(SwiftNativeFileSystemStore store)
    throws IOException {
    this.store = store;
  }

  /**
   * default class initialization
   *
   * @param fsuri  path to Swift
   * @param conf Hadoop configuration
   * @throws IOException
   */
  @Override
  public void initialize(URI fsuri, Configuration conf) throws IOException {
    super.initialize(fsuri, conf);
    setConf(conf);
    if (store == null) {
      store = new SwiftNativeFileSystemStore();
    }
    this.uri = fsuri;
    //the URI given maps to the x-ref in the config, that is retained
    //for visibility/comparison, but behind the scenes it is converted
    //into the references relative
    URI.create(String.format("%s://%s:%d/",
                             fsuri.getScheme(),
                             fsuri.getHost(),
                             fsuri.getPort()));
    this.workingDir =
      new Path("/user", System.getProperty("user.name")).makeQualified(this);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing SwiftNativeFileSystem against URI "+ uri
               + " and working dir " + workingDir);
    }
    store.initialize(uri, conf);
    LOG.debug("SwiftFileSystem initialized");

  }

  /**
   * @return path to Swift
   */
  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Path to user working directory
   *
   * @return Hadoop path
   */
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * @param dir user working directory
   */
  @Override
  public void setWorkingDirectory(Path dir) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.setWorkingDirectory to " + dir);
    }

    workingDir = dir;
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   * @return a FileStatus object
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    final FileStatus objectMetadata = store.getObjectMetadata(f);
    return objectMetadata;
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file.  For a nonexistent
   * file or regions, null will be returned.
   * <p/>
   * This call is most helpful with DFS, where it returns
   * hostnames of machines that contain the given file.
   * <p/>
   * The FileSystem will simply return an elt containing 'localhost'.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
                                               long start,
                                               long len) throws IOException {
    // Check if requested file in Swift is more than 5Gb. In this case
    // each block has its own location -which may be determinable
    // from the Swift client API, depending on the remote server

    final FileStatus[] listOfFileBlocks = store.listSubPaths(file.getPath());
    List<URI> locations = new ArrayList<URI>();
    if (listOfFileBlocks.length > 1) {
      for (FileStatus fileStatus : listOfFileBlocks) {
        if (SwiftObjectPath.fromPath(uri, fileStatus.getPath()).
          equals(SwiftObjectPath.fromPath(uri, file.getPath()))) {
          continue;
        }
        locations.addAll(store.getObjectLocation(fileStatus.getPath()));
      }
    } else {
      locations = store.getObjectLocation(file.getPath());
    }

    final String[] names = new String[locations.size()];
    final String[] hosts = new String[locations.size()];
    int i = 0;
    for (URI location : locations) {
      hosts[i] = location.getHost();
      names[i] = location.getAuthority();
      i++;
    }
    return new BlockLocation[]{
      new BlockLocation(names, hosts, 0, file.getLen())
    };
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.mkdirs: " + path);
    }
    Path absolutePath = makeAbsolute(path);
    //build a list of paths to create, with shortest one at the front
    List<Path> paths = new ArrayList<Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);

    boolean result = true;
    for (Path p : paths) {
      if (p.getParent() != null) {
        result &= mkdir(p);
      }
    }
    return result;
  }

  /**
   * internal implementation of directory creation
   *
   * @param path path to file
   * @return boolean file is created
   * @throws IOException if specified path is file instead of directory
   */
  private boolean mkdir(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    if (!store.objectExists(absolutePath)) {
      store.createDirectory(absolutePath);
    }
    //TODO: define a consistent semantic for a directory/subdirectory
    //in the hadoop:swift bridge. Hadoop FS assumes that there
    //are files and directories, whereas SwiftFS assumes
    //that there are just "objects"

/*
    FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(absolutePath);
      if (!fileStatus.isDir()) {
        throw new SwiftException(String.format(
          "Can't make directory for path '%s' since it exists and is not a directory: %s", 
          path, fileStatus));
      }
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Making dir '" + path + "' in Swift");
      }
      //file is not found: it must be created
      store.createDirectory(absolutePath);
    }
*/

    return true;
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given path
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.listStatus for: " + f);
    }
    return store.listSubPaths(f);
  }

  /**
   * This optional operation is not supported yet
   */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws
                                                                                  IOException {
    LOG.debug("SwiftFileSystem.append");
    throw new IOException("Not supported for Swift file system");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress)
    throws IOException {

    LOG.debug("SwiftFileSystem.create");
    FileStatus fileStatus = null;
    try {
      fileStatus = getFileStatus(makeAbsolute(file));
    } catch (FileNotFoundException e) {
      //nothing to do
    }
    if (fileStatus != null && !fileStatus.isDir()) {
      if (overwrite) {
        delete(file, true);
      } else {
        throw new IOException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new IOException("Mkdirs failed to create " + parent.toString());
        }
      }
    }

    SwiftNativeOutputStream out = new SwiftNativeOutputStream(getConf(),
                                                              store,
                                                              file.toUri()
                                                                  .toString());
    return new FSDataOutputStream(out, statistics);
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param path       the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return new FSDataInputStream(
      new BufferedFSInputStream(
        new SwiftNativeInputStream(store, statistics, path), bufferSize));
  }

  /**
   * Renames Path src to Path dst. On swift this uses copy-and-delete
   * and <i>is not atomic</i>.
   *
   * @param src path
   * @param dst path
   * @return true if directory renamed, false otherwise
   * @throws IOException on problems
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    return store.renameDirectory(src, dst);
  }


  /**
   * Delete a file or directory
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception if the
   *                  directory is not empty
   *                  case of a file the recursive can be set to either true or false.
   * @return true if a file was found and deleted 
   * @throws IOException
   */
  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    LOG.debug("SwiftFileSystem.delete");
    Path absolutePath = makeAbsolute(path);
    final FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(path);
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Delete called for '" + path +
                  "' but file does not exist, so returning false");
      }
      return false;
    }
    if (!fileStatus.isDir()) {
      //simple file: delete it
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting file '" + path + "'");
      }
      store.deleteObject(absolutePath);
    } else {
      //it's a directory
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting directory '" + path + "'");
      }
      FileStatus[] contents = listStatus(absolutePath);
      if (contents == null) {
        //the directory went away during the non-atomic stages of the operation.
        // Return false as it was not this thread doing the deletion.
        return false;
      }
      if ((contents.length != 0) && (!recursive)) {
        throw new IOException("Directory " + path.toString()
                              + " is not empty.");
      }
      for (FileStatus p : contents) {
        if (!delete(p.getPath(), recursive)) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Makes path absolute
   *
   * @param path path to file
   * @return absolute path
   */
  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }
}
