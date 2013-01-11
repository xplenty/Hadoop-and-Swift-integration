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

package org.apache.hadoop.fs.swift.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.SwiftTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestSwiftRestClient {

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
    if (runTests) {
      SwiftRestClient client = createClient();
    }
  }

  private SwiftRestClient createClient() throws IOException {
    return SwiftRestClient.getInstance(serviceURI, conf);
  }


  @Test
  public void testAuthenticate() throws Throwable {
    if (runTests) {
      SwiftRestClient client = createClient();
      client.authenticate();
    }
  }


}
