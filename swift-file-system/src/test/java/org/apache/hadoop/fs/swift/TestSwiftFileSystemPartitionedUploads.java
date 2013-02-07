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
package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftFileSystemForFunctionalTests;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * these tests currently are unit tests, but will be
 * moved to functional/integration tests
 */
public class TestSwiftFileSystemPartitionedUploads extends SwiftFileSystemBaseTest {

  private URI uri;
  private Configuration conf;
  private SwiftFileSystemForFunctionalTests swiftFS;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    uri = getFilesystemURI();
    conf = new Configuration();
    //patch the configuration with the factory of the new driver


    swiftFS = new SwiftFileSystemForFunctionalTests();
    swiftFS.setPartitionSize(1024L);
    fs = swiftFS;
    fs.initialize(uri, conf);
  }


  @Override
  protected SwiftNativeFileSystem createSwiftFS() throws IOException {
    swiftFS = new SwiftFileSystemForFunctionalTests();
    swiftFS.setPartitionSize(1024L);
    return swiftFS;
  }

  @After
  public void tearDown() throws Exception {
    SwiftTestUtils.cleanupInTeardown(fs, "/test");
  }

  protected URI getFilesystemURI() throws URISyntaxException, IOException {
    return SwiftTestUtils.getServiceURI(new Configuration());
  }

  /**
   * tests functionality for big files ( > 5Gb) upload
   */
  @Test
  public void testFilePartUpload() throws IOException, URISyntaxException {

    final Path path = new Path("/test/huge-file");

    int len = 4096;
    final byte[] src = SwiftTestUtils.dataset(len,32,144);
    FSDataOutputStream out = fs.create(path, false,
                                       fs.getConf()
                                         .getInt("io.file.buffer.size",
                                                 4096),
                                       (short) 1,
                                       1024);
    assertEquals("wrong number of partitons written",
                 0, swiftFS.getPartitionsWritten(out));
    //write first half
    out.write(src, 0, len / 2);
    assertEquals("wrong number of partitons written",
                 1, swiftFS.getPartitionsWritten(out));
    //write second half
    out.write(src, len / 2, len / 2);
    assertEquals("wrong number of partitons written",
                 2, swiftFS.getPartitionsWritten(out));
    out.close();
    assertEquals("wrong number of partitons written",
                 3, swiftFS.getPartitionsWritten(out));

    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", len, fs.getFileStatus(path).getLen());

    FSDataInputStream in = fs.open(path);
    byte[] dest = new byte[len];
    in.readFully(0, dest);
    in.close();

    SwiftTestUtils.compareByteArrays(src, dest, len);

  }


}
