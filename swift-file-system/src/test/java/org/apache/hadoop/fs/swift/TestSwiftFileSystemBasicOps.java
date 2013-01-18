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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

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


  @Test
  public void testCreate() throws Throwable {
    if (!runTests) {
      return;
    }
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
    try {
      writeTextFile(fs, path, "Testing a put to a file", false);
    } catch (Exception e) {
      caught = e;
    }
    try {
      fs.delete(path, true);
    } catch (IOException e) {
      LOG.error("during closedown");
      if (caught == null) {
        caught = e;
      }
    }
    if (caught != null) {
      throw caught;
    }
  }

  private void writeTextFile(SwiftNativeFileSystem fs,
                             Path path,
                             String text,
                             boolean overwrite) throws IOException {
    FSDataOutputStream stream = fs.create(path, overwrite);
    stream.write(SwiftTestUtils.toAsciiByteArray(text));
    stream.close();
  }

  @Test
  public void testPutGetFile() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testPutGetFile");
    try {
      String text = "Testing a put and get to a file "
                 + System.currentTimeMillis();
      writeTextFile(fs, path, text, false);

      FSDataInputStream in = fs.open(path);
      byte[] buf = new byte[text.length()];
      in.readFully(0, buf);
      in.close();
      assertEquals(text, SwiftTestUtils.toChar(buf));
    } finally {
      fs.delete(path, false);
    }
  }


  @Test
  public void testOverwrite() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testOverwrite");
    try {
      String text = "Testing a put to a file "
                    + System.currentTimeMillis();
      writeTextFile(fs, path, text, false);
      String text2 = "Overwriting a file "
                     + System.currentTimeMillis();
      writeTextFile(fs, path, text2, true);

      FSDataInputStream in = fs.open(path);
      byte[] buf = new byte[text.length()];
      in.readFully(0, buf);
      in.close();
      assertEquals(text2, SwiftTestUtils.toChar(buf));
    } finally {
      fs.delete(path, false);
    }
  }

  @Test
  public void testFileStatus() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testFileStatus");
    try {
      String text = "Testing File Status "
                    + System.currentTimeMillis();
      writeTextFile(fs, path, text, false);
      FileStatus fileStatus = fs.getFileStatus(path);
      assertTrue("Not a file: "+fileStatus, fileStatus.isFile());
      assertFalse("A dir: "+fileStatus, fileStatus.isDirectory());
    } finally {
      fs.delete(path, false);
    }
  }
  @Test
  public void testDirStatus() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testDirStatus");
    try {
      fs.mkdirs(path);
      FileStatus fileStatus = fs.getFileStatus(path);
      assertFalse("A file: "+fileStatus, fileStatus.isFile());
      assertTrue("Not a a dir: "+fileStatus, fileStatus.isDirectory());
    } finally {
      fs.delete(path, false);
    }
  }


}
