/*
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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.exceptions.SwiftNotDirectoryException;
import org.apache.hadoop.fs.swift.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swift.exceptions.SwiftUnsupportedFeatureException;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftUtils;
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
   * This constructor used for testing purposes
   */
  public SwiftNativeFileSystem(SwiftNativeFileSystemStore store)
          throws IOException {
    this.store = store;
  }

  /**
   * This is for testing
   * @return the inner store class
   */
  @InterfaceAudience.Private
  public SwiftNativeFileSystemStore getStore() {
    return store;
  }

  /**
   * default class initialization
   *
   * @param fsuri path to Swift
   * @param conf  Hadoop configuration
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
    this.workingDir = new Path("/user",
            System.getProperty("user.name")).makeQualified(uri, new Path(System.getProperty("user.name")));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing SwiftNativeFileSystem against URI " + uri
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

    return store.getObjectMetadata(f);
  }


  @Override
  public boolean isFile(Path f) throws IOException {

    try {
      FileStatus fileStatus = getFileStatus(f);
      return !SwiftUtils.isDirectory(fileStatus);
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {

    try {
      FileStatus fileStatus = getFileStatus(f);
      return SwiftUtils.isDirectory(fileStatus);
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
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
//    return super.getFileBlockLocations(file, start, len);
    // Check if requested file in Swift is more than 5Gb. In this case
    // each block has its own location -which may be determinable
    // from the Swift client API, depending on the remote server
    final FileStatus[] listOfFileBlocks = store.listSubPaths(file.getPath(),
                                                             false,
                                                             true);
    List<URI> locations = new ArrayList<URI>();
    if (listOfFileBlocks.length > 1) {
      for (FileStatus fileStatus : listOfFileBlocks) {
        if (SwiftObjectPath.fromPath(uri, fileStatus.getPath())
                .equals(SwiftObjectPath.fromPath(uri, file.getPath()))) {
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
    while (!absolutePath.isRoot()) {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    }

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
    
    FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(absolutePath);
      if (!SwiftUtils.isDirectory(fileStatus)) {
        throw new SwiftNotDirectoryException(path,
                String.format(": can't mkdir since it is not a directory: %s",
                        fileStatus));
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping mkdir(" + path + ") as it exists already");
        }
      }
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Making dir '" + path + "' in Swift");
      }
      //file is not found: it must be created
      store.createDirectory(absolutePath);
    }
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
    return store.listSubPaths(f, false, false);
  }

  /**
   * This optional operation is not supported yet
   */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws
          IOException {
    LOG.debug("SwiftFileSystem.append");
    throw new SwiftUnsupportedFeatureException("Not supported: append()");
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
    if (fileStatus != null && !SwiftUtils.isDirectory(fileStatus)) {
      if (overwrite) {
        delete(file, true);
      } else {
        throw new SwiftException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null && !parent.isRoot()) {
        if (!mkdirs(parent)) {
          throw new SwiftException("Mkdirs failed to create " + parent.toString());
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

  private static String pathToKey(Path path) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      // allow uris without trailing slash after bucket to refer to root,
      // like s3n://mybucket
      return "";
    }
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    String ret = path.toUri().getPath().substring(1); // remove initial slash
    if (ret.endsWith("/") && (ret.indexOf("/") != ret.length() - 1)) {
      ret = ret.substring(0, ret.length() - 1);
    }
    return path.toUri().getPath();
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

    try {
      store.rename(makeAbsolute(src), makeAbsolute(dst));
      //success
      return true;
    } catch (SwiftOperationFailedException e) {
      //downgrade to a failure
      return false;
    } catch (FileNotFoundException e) {
      //downgrade to a failure
      return false;
    }
  }

/*
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    //this rename is from S3 after the HADOOP-9265 changes
    String srcKey = pathToKey(makeAbsolute(src));
    final String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";


    if (srcKey.isEmpty()) {
      // Cannot rename root of file system
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble +
                  "returning false as cannot rename the root of a filesystem");
      }
      return false;
    }

    //get status of source
    boolean srcIsFile;
    try {
      srcIsFile = getFileStatus(src).isFile();
    } catch (FileNotFoundException e) {
      //bail out fast if the source does not exist
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "returning false as src does not exist");
      }
      return false;
    }
    // Figure out the final destination
    String dstKey = pathToKey(makeAbsolute(dst));
    //check for rename here, before probes for file type take place
    // -this is needed to stop rename(dir,dir) creating the path dir/dir
    // and then rejectig that operation

    if (srcKey.equals(dstKey)) {
      //fully resolved destination key matches source: fail
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "renamingToSelf; returning true");
      }
      return true;
    }

    try {
      boolean dstIsFile = getFileStatus(dst).isFile();
      if (dstIsFile) {
        //destination is a file.
        //you can't copy a file or a directory onto an existing file
        //except for the special case of dest==src
        if (LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble +
                    "returning dst is an already existing file");
        }
        return srcKey.equals(dstKey);
      } else {
        //destination exists and is a directory
        if (LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble + "using dst as output directory");
        }
        //destination goes under the dst path, with the name of the
        //source entry
        dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      }
    } catch (FileNotFoundException e) {
      //destination does not exist => the source file or directory
      //is copied over with the name of the destination
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "using dst as output destination");
      }
      try {
        if (getFileStatus(dst.getParent()).isFile()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(debugPreamble +
                      "returning false as dst parent exists and is a file");
          }
          return false;
        }
      } catch (FileNotFoundException ex) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble +
                    "returning false as dst parent does not exist");
        }
        return false;
      }
    }

    //rename to self behavior follows Posix rules and is different
    //for directories and files -the return code is driven by src type
    if (srcKey.equals(dstKey)) {
      //fully resolved destination key matches source: fail
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "renamingToSelf; returning true");
      }
      return true;
    }

    //here all path checks are complete, srcKey and dstKey are ready for use

    if (srcIsFile) {
      //source is a file; COPY then DELETE
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble +
                  "src is file, so doing copy then delete in S3");
      }
      store.copy(srcKey, dstKey);
      store.delete(srcKey);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "src is directory, so copying contents");
      }
      //Verify dest is not a child of the parent
      if (dstKey.startsWith(srcKey + "/")) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            debugPreamble + "cannot rename a directory to a subdirectory");
        }
        return false;
      }
      store.storeEmptyFile(dstKey + FOLDER_SUFFIX);

      List<String> keysToDelete = new ArrayList<String>();
      String priorLastKey = null;
      do {
        PartialListing listing =
          store.list(srcKey, S3_MAX_LISTING_LENGTH, priorLastKey, true);
        for (FileMetadata file : listing.getFiles()) {
          keysToDelete.add(file.getKey());
          store.copy(file.getKey(),
                     dstKey + file.getKey().substring(srcKey.length()));
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);

      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble +
                  "all files in src copied, now removing src files");
      }
      for (String key : keysToDelete) {
        store.delete(key);
      }

      try {
        store.delete(srcKey + FOLDER_SUFFIX);
      } catch (FileNotFoundException e) {
        //this is fine, we don't require a marker
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "done");
      }
    }

    return true;
  }

*/

  /**
   * Delete a file or directory
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception if the
   *                  directory is not empty
   *                  case of a file the recursive can be set to either true or false.
   * @return true if the object was deleted
   * @throws IOException IO problems
   */
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      return innerDelete(path, recursive);
    } catch (FileNotFoundException e) {
      //base path was not found.
      return false;
    }
  }

  /**
   * Delete a file
   * @deprecated Use {@link #delete(Path, boolean)} instead.
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
    return delete(f, true);
  }

  /**
   * Delete the entire tree. This is an internal one with slightly different
   * behavior: if an entry is missing, a {@link FileNotFoundException} is
   * raised. This lets the caller distinguish a file not found with
   * other reasons for failure, so handles race conditions in recursive
   * directory deletes better.
   * <p/>
   * The problem being addressed is: caller A requests a recursive directory
   * of directory /dir ; caller B requests a delete of a file /dir/file,
   * between caller A enumerating the files contents, and requesting a delete
   * of /dir/file. We want to recognise the special case
   * "directed file is no longer there" and not convert that into a failure
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception if the
   *                  directory is not empty
   *                  case of a file the recursive can be set to either true or false.
   * @return true if the object was deleted
   * @throws IOException           IO problems
   * @throws FileNotFoundException if a file/dir being deleted is not there -
   *                               this includes entries below the specified path, (if the path is a dir
   *                               and recursive is true)
   */
  private boolean innerDelete(Path path, boolean recursive) throws IOException {
    Path target = makeAbsolute(path);
    final FileStatus fileStatus;
    fileStatus = getFileStatus(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting path '" + path + "'");
    }
    if (!SwiftUtils.isDirectory(fileStatus)) {
      //simple file: delete it
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting simple file '" + path + "'");
      }
      store.deleteObject(target);
    } else {
      //it's a directory
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting directory '" + path + "'");
      }

      //get all entries
      List<FileStatus> children = store.listDirectory(target, true, true);

      //look to see if there are now any children
      if (!children.isEmpty() && !recursive) {
        //if there are children, unless this is a recursive operation, fail immediately
        throw new SwiftOperationFailedException("Directory " + path + " is not empty.");
      }

      //delete the children
      for (FileStatus child : children) {
        Path childPath = child.getPath();
        try {
          store.deleteObject(childPath);
        } catch (FileNotFoundException e) {
          //the path went away -race conditions.
          //do not fail, as the outcome is still OK.
          LOG.info("Path " + childPath + " is no longer present");
        }
      }
      //here any children that existed have been deleted
      //so rm the directory (which is a no-op for /)
      store.rmdir(target);
    }

    return true;
  }

  /**
   * Makes path absolute
   *
   * @param path path to file
   * @return absolute path
   */
  protected Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }
}
