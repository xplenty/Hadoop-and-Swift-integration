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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.swift.exceptions.SwiftBadRequestException;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;

import static org.junit.Assert.*;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class TestSwiftFileSystemBasicOps {

  private static final Log LOG =
    LogFactory.getLog(TestSwiftFileSystemBasicOps.class);

  private Configuration conf;
  private boolean runTests;
  private URI serviceURI;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    runTests = SwiftTestUtils.hasServiceURI(conf);
    if (runTests) {
      serviceURI = SwiftTestUtils.getServiceURI(conf);
    }
  }

  protected void assumeTestEnabled() {
    Assume.assumeTrue(runTests);
  }

  @Test
  public void testCreate() throws Throwable {
    assumeTestEnabled();
    createInitedFS();
  }

  private SwiftNativeFileSystem createInitedFS() throws IOException {
    SwiftNativeFileSystem fs = new SwiftNativeFileSystem();
    fs.initialize(serviceURI, conf);
    return fs;
  }


  @Test
  public void testListRoot() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    RemoteIterator<LocatedFileStatus> files =
      fs.listFiles(new Path("/"), true);
    while (files.hasNext()) {
      LocatedFileStatus status = files.next();
      LOG.info(status.toString());
    }
  }


  @Test
  public void testlsRoot() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/");
    FileStatus[] statuses = fs.listStatus(path);
  }


  @Test
  public void testMkDir() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testMkDir");
    fs.mkdirs(path);
    //success then -so try a recursive operation
    fs.delete(path, true);
  }


  @Test
  public void testDeleteNonexistentFile() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testDeleteNonexistentFile");
    assertFalse("delete returned true", fs.delete(path, false));
  }


  @Test
  public void testPutFile() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testPutFile");
    Exception caught = null;
    SwiftTestUtils.writeTextFile(fs, path, "Testing a put to a file", false);
    SwiftTestUtils.assertDeleted(fs, path, false);
  }

  @Test
  public void testPutGetFile() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testPutGetFile");
    try {
      String text = "Testing a put and get to a file "
                    + System.currentTimeMillis();
      SwiftTestUtils.writeTextFile(fs, path, text, false);

      String result = SwiftTestUtils.readBytesToString(fs, path, text.length());
      assertEquals(text, result);
    } finally {
      delete(fs, path);
    }
  }

  @Test
  public void testPutDeleteFileInSubdir() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path =
      new Path("/testPutDeleteFileInSubdir/testPutDeleteFileInSubdir");
    String text = "Testing a put and get to a file in a subdir "
                  + System.currentTimeMillis();
    SwiftTestUtils.writeTextFile(fs, path, text, false);
    SwiftTestUtils.assertDeleted(fs, path, false);
    //now delete the parent that should have no children
    SwiftTestUtils.assertDeleted(fs, new Path("/testPutDeleteFileInSubdir"),
                                 false);
  }

  @Test
  public void testRecursiveDelete() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path childpath =
      new Path("/testRecursiveDelete/testRecursiveDelete");
    String text = "Testing a put and get to a file in a subdir "
                  + System.currentTimeMillis();
    SwiftTestUtils.writeTextFile(fs, childpath, text, false);
    //now delete the parent that should have no children
    SwiftTestUtils.assertDeleted(fs, new Path("/testRecursiveDelete"), true);
    assertFalse("child entry still present " + childpath, fs.exists(childpath));
  }

  private void delete(SwiftNativeFileSystem fs, Path path) {
    try {
      if (!fs.delete(path, false)) {
        LOG.warn("Failed to delete " + path);
      }
    } catch (IOException e) {
      LOG.warn("deleting " + path, e);
    }
  }

  private void deleteR(SwiftNativeFileSystem fs, Path path) {
    try {
      if (!fs.delete(path, true)) {
        LOG.warn("Failed to delete " + path);
      }
    } catch (IOException e) {
      LOG.warn("deleting " + path, e);
    }
  }

  @Test
  public void testOverwrite() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testOverwrite");
    try {
      String text = "Testing a put to a file "
                    + System.currentTimeMillis();
      SwiftTestUtils.writeTextFile(fs, path, text, false);
      SwiftTestUtils.assertFileLength(fs, path, text.length());
      String text2 = "Overwriting a file "
                     + System.currentTimeMillis();
      SwiftTestUtils.writeTextFile(fs, path, text2, true);
      SwiftTestUtils.assertFileLength(fs, path, text2.length());


      String result = SwiftTestUtils.readBytesToString(fs, path, text2.length());
      assertEquals(text2, result);
    } finally {
      delete(fs, path);
    }
  }

  @Test
  public void testFileStatus() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testFileStatus");
    try {
      String text = "Testing File Status "
                    + System.currentTimeMillis();
      SwiftTestUtils.writeTextFile(fs, path, text, false);
      FileStatus fileStatus = fs.getFileStatus(path);
      assertTrue("Not a file: " + fileStatus, fileStatus.isFile());
      assertFalse("A dir: " + fileStatus, fileStatus.isDirectory());
    } finally {
      delete(fs, path);
    }
  }

  /**
   * Assert that a newly created directory is a directory
   * @throws Throwable if not, or if something else failed
   */
  @Test
  public void testDirStatus() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testDirStatus");
    try {
      fs.mkdirs(path);
      SwiftTestUtils.assertDirectory(fs, path);
    } finally {
      delete(fs, path);
    }
  }

  /**
   * Assert that if a directory that has children is deleted, it is still
   * a directory
   * @throws Throwable if not, or if something else failed
   */
  @Test
  public void testDirStaysADir() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testDirStatus");
    Path child = new Path(path, "child");
    try {
      //create the dir
      fs.mkdirs(path);
      //create the child dir
      SwiftTestUtils.writeTextFile(fs, child, "child file", true);
      //assert the parent has the directory nature
      SwiftTestUtils.assertDirectory(fs, path);
      //now rm the child
      delete(fs, child);
    } finally {
      deleteR(fs, path);
    }
  }

  @Test
  public void testCreateMultilevelDir() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();

    Path base = new Path("/testCreateMultilevelDir");
    Path path = new Path(base, "1/2/3");
    fs.mkdirs(path);
    fs.delete(base, true);
  }

  @Test
  public void testCreateDirWithFileParent() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();

    Path path = new Path("/testCreateDirWithFileParent");
    Path child = new Path(path, "subdir/child");
    fs.mkdirs(path);
    try {
      //create the child dir
      SwiftTestUtils.writeTextFile(fs, path, "parent", true);
      try {
        fs.mkdirs(child);
      } catch (SwiftException expected) {
        LOG.debug("Expecte Exception", expected);
      }
    } finally {
      fs.delete(path, true);
    }

  }


  @Test
  public void testRenameMissingFile() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testRenameMissingFile");
    Path path2 = new Path("/testRenameMissingFileDest");
    try {
      fs.rename(path, path2);
      fail("Expected rename of a missing file to fail");
    } catch (FileNotFoundException fnfe) {
      //success
    } finally {
      delete(fs, path);
      delete(fs, path2);
    }
  }

  @Test
  public void testLongObjectNamesForbidden() throws Throwable {
    StringBuilder buffer = new StringBuilder(1200);
    buffer.append("/");
    for (int i = 0; i < (1200 / 4); i++) {
      buffer.append(String.format("%04x", i));
    }
    SwiftNativeFileSystem fs = createInitedFS();
    String pathString = buffer.toString();
    Path path = new Path(pathString);
    try {
      SwiftTestUtils.writeTextFile(fs, path, pathString, true);
      //if we get here, problems.
      LOG.warn("Managed to create an object with a name of length "
               + pathString.length());
      fs.delete(path, false);
    } catch (SwiftBadRequestException e) {
      //expected
      LOG.debug("Caught exception " + e, e);
    }
  }

  @Test
  public void testLsNonExistentFile() throws Exception {
    SwiftNativeFileSystem fs = createInitedFS();
    try {
      Path path = new Path("/test/hadoop/file");
      FileStatus[] statuses = fs.listStatus(path);
      fail("Should throw FileNotFoundException on " + path
           + " but got list of length " + statuses.length);
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

}
