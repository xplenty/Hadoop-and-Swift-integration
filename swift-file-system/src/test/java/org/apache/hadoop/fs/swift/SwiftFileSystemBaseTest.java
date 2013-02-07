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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystemStore;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SwiftFileSystemBaseTest {
  protected static final Log LOG =
    LogFactory.getLog(TestSwiftFileSystemExtendedContract.class);
  protected SwiftNativeFileSystem fs;
  protected byte[] data = SwiftTestUtils.dataset(getBlockSize() * 2, 0, 255);

  @Before
  public void setUp() throws Exception {
    SwiftTestUtils.noteAction("setup");
    final URI uri = getFilesystemURI();
    final Configuration conf = new Configuration();

    fs = createSwiftFS();
    fs.initialize(uri, conf);
    SwiftTestUtils.noteAction("setup complete");
  }

  @After
  public void tearDown() throws Exception {
    SwiftTestUtils.cleanupInTeardown(fs, "/test");
  }

  protected URI getFilesystemURI() throws URISyntaxException, IOException {
    return SwiftTestUtils.getServiceURI(new Configuration());
  }

  protected SwiftNativeFileSystem createSwiftFS() throws IOException {
    SwiftNativeFileSystem swiftNativeFileSystem =
      new SwiftNativeFileSystem();
    return swiftNativeFileSystem;
  }

  protected int getBlockSize() {
    return 1024;
  }

  protected boolean renameSupported() {
    return true;
  }

  protected void assumeRenameSupported() {
    Assume.assumeTrue(renameSupported());
  }

  protected Path path(String pathString) {
    return new Path(pathString).makeQualified(fs);
  }

  protected void createFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.write(data, 0, data.length);
    out.close();
  }
  protected void createEmptyFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.close();
  }

  /**
   * Get the inner store -useful for lower level operations
   * @return the store
   */
  protected SwiftNativeFileSystemStore getStore() {
    return fs.getStore();
  }

  protected void rename(Path src, Path dst, boolean renameMustSucceed,
                        boolean srcExists, boolean dstExists) throws IOException {
    if (renameMustSucceed) {
      renameToSuccess(src, dst, srcExists, dstExists);
    } else {
      renameToFailure(src, dst);
    }
  }

  private String getRenameOutcome(Path src, Path dst) throws IOException {
    String lsDst = ls(dst);
    Path parent = dst.getParent();
    String lsParent = parent != null ? ls(parent) : "";
    return "  result of " + src + " => " + dst
                     + " - " + lsDst
                     + " \n" + lsParent;
  }

  /**
   * Rename, expecting an exception to be thrown
   * @param src source
   * @param dst dest
   * @throws IOException a failure other than an
   * expected SwiftRenameException or FileNotFoundException
   */
  protected void renameToFailure(Path src, Path dst) throws IOException {
    try {
      getStore().rename(src, dst);
      fail("Expected failure renaming " + src + " to " + dst
           +"- but got success");
    } catch (SwiftOperationFailedException e) {
      LOG.debug("Rename failed (expected):" + e);
    } catch (FileNotFoundException e) {
      LOG.debug("Rename failed (expected):" + e);
    }
  }

  /**
   * Rename, expecting an exception to be thrown
   * @param src source
   * @param dst dest
   * @throws SwiftOperationFailedException
   * @throws IOException
   */
  protected void renameToSuccess(Path src, Path dst,
                                 boolean srcExists, boolean dstExists)
      throws SwiftOperationFailedException, IOException {
    getStore().rename(src, dst);
    String outcome = getRenameOutcome(src, dst);
    assertEquals("Source " + src + "exists: " + outcome,
                 srcExists, fs.exists(src));
    assertEquals("Destination " + dstExists + " exists" + outcome,
                 dstExists, fs.exists(dst));
  }

  protected String ls(Path path) throws IOException {
    return SwiftTestUtils.ls(fs, path);
  }

  public void assertExists(String message, Path path) throws IOException {
    SwiftTestUtils.assertPathExists(message, fs, path);
  }

  public void assertPathDoesNotExist(String message, Path path) throws IOException {
    SwiftTestUtils.assertPathDoesNotExist(message, fs, path);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   * @param filename name of the file
   * @throws IOException IO problems during file operations
   */
  protected void assertIsFile(Path filename) throws IOException {
    SwiftTestUtils.assertIsFile(fs, filename);
  }


  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   * @param filename name of the file
   * @throws IOException IO problems during file operations
   */
  protected void mkdirs(Path path) throws IOException {
    assertTrue("Failed to mkdir" + path, fs.mkdirs(path));
  }


  protected void assertDeleted(Path file, boolean recursive) throws IOException {
    assertExists("about to be deleted file", file);
    boolean deleted = fs.delete(file, recursive);
    String dir = ls(file.getParent());
    assertTrue("Delete failed on " + file + ": " + dir, deleted);
    assertPathDoesNotExist("Deleted file", file);
  }
}
