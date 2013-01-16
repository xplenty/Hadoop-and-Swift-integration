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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestSwiftFileSystemBasicOps {
  private static final Log LOG = LogFactory.getLog(TestSwiftFileSystemBasicOps.class);

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
  public void testPutFile() throws Throwable {
    SwiftNativeFileSystem fs = createInitedFS();
    Path path = new Path("/testPutFile");
    FSDataOutputStream stream = fs.create(path, false);
    stream.writeChars("Testing a put to a file");
    stream.close();
    fs.delete(path, true);
  }


}
