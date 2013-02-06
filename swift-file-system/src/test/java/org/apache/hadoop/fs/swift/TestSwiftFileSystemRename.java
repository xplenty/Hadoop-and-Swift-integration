/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.http.RestClientBindings;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSwiftFileSystemRename extends SwiftFileSystemBaseTest {


  /**
   * Rename a file into a directory
   * @throws Exception
   */
  @Test
  public void testRenameFileIntoExistingDirectory() throws Exception {
    assumeRenameSupported();

    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    Path newFile = path("/test/new/newdir/file");
    if (!fs.exists(newFile)) {
      String ls = ls(dst);
      LOG.info(ls(path("/test/new")));
      LOG.info(ls(path("/test/hadoop")));
      fail("did not find " + newFile + " - directory: " + ls);
    }
    assertTrue("Destination changed",
               fs.exists(path("/test/new/newdir/file")));
  }


  @Test
  public void testRenameFile() throws Exception {
    assumeRenameSupported();

    final Path old = new Path("/test/alice/file");
    final Path newPath = new Path("/test/bob/file");
    fs.mkdirs(newPath.getParent());
    final FSDataOutputStream fsDataOutputStream = fs.create(old);
    final byte[] message = "Some data".getBytes();
    fsDataOutputStream.write(message);
    fsDataOutputStream.close();

    assertTrue(fs.exists(old));
    rename(old, newPath, true, false, true);

    final FSDataInputStream open = fs.open(newPath);
    final byte[] bytes = new byte[512];
    final int read = open.read(bytes);
    final byte[] buffer = new byte[read];
    System.arraycopy(bytes, 0, buffer, 0, read);
    assertEquals(new String(message), new String(buffer));
  }

  @Test
  public void testRenameDirectory() throws Exception {
    assumeRenameSupported();

    final Path old = new Path("/test/data/logs");
    final Path newPath = new Path("/test/var/logs");
    fs.mkdirs(old);
    fs.mkdirs(newPath.getParent());
    assertTrue(fs.exists(old));
    rename(old, newPath, true, false, true);
  }

  @Test
  public void testRenameTheSameDirectory() throws Exception {
    assumeRenameSupported();

    final Path old = new Path("/test/usr/data");
    fs.mkdirs(old);
    rename(old, old, false, true, true);
  }

  @Test
  public void testRenameDirectoryIntoExistingDirectory() throws Exception {
    assumeRenameSupported();

    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));

    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    //this renames into a child
    rename(src, dst, true, false, true);
    assertExists("new dir", path("/test/new/newdir/dir"));
    assertExists("Renamed nested file1", path("/test/new/newdir/dir/file1"));
    assertPathDoesNotExist("Nested file1 should have been deleted",
                           path("/test/hadoop/dir/file1"));
    assertExists("Renamed nested subdir",
                 path("/test/new/newdir/dir/subdir/"));
    assertExists("file under subdir",
                 path("/test/new/newdir/dir/subdir/file2"));

    assertPathDoesNotExist("Nested /test/hadoop/dir/subdir/file2 still exists",
                path("/test/hadoop/dir/subdir/file2"));
  }

  /**
   * trying to rename a directory onto itself should fail,
   * preserving everything underneath.
   */
  @Test
  public void testRenameDirToSelf() throws Throwable {
    assumeRenameSupported();
    Path parentdir = path("test/parentdir");
    fs.mkdirs(parentdir);
    Path child = new Path(parentdir, "child");
    createFile(child);

    rename(parentdir, parentdir, false, true, true);
    //verify the child is still there
    assertIsFile(child);
  }

  /**
   * Assert that root directory renames are not allowed
   * @throws Exception on failures
   */
  @Test
  public void testRenameRootDirForbidden() throws Exception {
    assumeRenameSupported();
    rename(path("/"),
           path("/test/newRootDir"),
           false, true, false);
  }

  /**
   * Assert that renaming a parent directory to be a child
   * of itself is forbidden
   * @throws Exception on failures
   */
  @Test
  public void testRenameChildDirForbidden() throws Exception {
    assumeRenameSupported();

    Path parentdir = path("/test/parentdir");
    fs.mkdirs(parentdir);
    Path childFile = new Path(parentdir, "childfile");
    createFile(childFile);
    //verify one level down
    Path childdir = new Path(parentdir, "childdir");
    rename(parentdir, childdir, false, true, false);
    //now another level
    fs.mkdirs(childdir);
    Path childchilddir = new Path(childdir, "childdir");
    rename(parentdir, childchilddir, false, true, false);
  }


  @Test
  public void testRenameDirWithSubDirs() throws IOException {
    assumeRenameSupported();
    final FileSystem fileSystem = fs;

    final String message = "message";
    final Path filePath = new Path("/home/user/documents/file.txt");
    final Path newFilePath = new Path("/home/user/files/file.txt");

    final FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();

    fileSystem.rename(filePath, newFilePath);

    final FSDataInputStream inputStream = fileSystem.open(newFilePath);
    final byte[] data = new byte[20];
    final int read = inputStream.read(data);

    assertEquals(message.length(), read);
    assertEquals(message, new String(data, 0, read));
  }


}
