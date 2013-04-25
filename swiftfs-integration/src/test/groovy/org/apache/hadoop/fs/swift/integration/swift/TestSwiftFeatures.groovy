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

package org.apache.hadoop.fs.swift.integration.swift

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.swift.exceptions.SwiftPathExistsException
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants
import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.hadoop.fs.swift.integration.tools.DataGenerator
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystemStore
import org.apache.hadoop.fs.swift.util.SwiftTestUtils
import org.junit.Assume
import org.junit.Test

/**
 * Generate a swift doc
 */
@Commons
class TestSwiftFeatures extends IntegrationTestBase {

  @Test
  public void testPartionInfoWorking() throws Throwable {
    FileSystem fs = getSwiftFS()
    Configuration fsconf = fs.getConf();
    SwiftNativeFileSystemStore store = fs.getStore()
    int partSize = fsconf.getInt(SwiftProtocolConstants.SWIFT_PARTITION_SIZE, 0)
    assert partSize != 0
    assert partSize == store.getPartsizeKB();
  }

  @Test
  public void testBlocksizeInfoWorking() throws Throwable {
    FileSystem fs = getSwiftFS()
    Configuration fsconf = fs.getConf();
    SwiftNativeFileSystemStore store = fs.getStore()
    int size = fsconf.getInt(SwiftProtocolConstants.SWIFT_BLOCKSIZE, 0)
    assert size != 0
    assert size*1024 == store.getBlocksize();
  }

  def SwiftNativeFileSystem getSwiftFS() {
    FileSystem fs = getDestFilesystem();
    Assume.assumeTrue(fs instanceof SwiftNativeFileSystem)
    (SwiftNativeFileSystem)fs;
  }


}
