

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

package org.apache.hadoop.fs.swift.integration

import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.*
import org.junit.Assert
import org.apache.hadoop.fs.swift.util.SwiftTestUtils
import groovy.util.logging.Commons
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem
import org.apache.hadoop.conf.Configuration

@Commons
class IntegrationTestBase extends Assert {
  /**
   * name of the key in the config XML files defining the filesystem to work with
   */
  public static final String KEY_TEST_FS = SwiftTestUtils.TEST_FS_SWIFT;

  protected SwiftNativeFileSystem bindFilesystem() {
    def conf = new Configuration();
    def serviceURI = SwiftTestUtils.getServiceURI(conf);
    SwiftNativeFileSystem fs = new SwiftNativeFileSystem();
    fs.initialize(serviceURI, conf);
    fs
  }
  
}
