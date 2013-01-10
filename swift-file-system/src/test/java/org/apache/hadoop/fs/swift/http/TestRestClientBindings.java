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

import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.*;
import static org.apache.hadoop.fs.swift.SwiftTestUtils.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class TestRestClientBindings extends Assert {

  private static final String HOSTNAME = "hostname";
  private static final String FS_URI = "swift://" + HOSTNAME + "/";
  private static final String AUTH_URL = "http://localhost:8080/auth";
  private static final String USER = "user";
  private static final String PASS = "pass";
  private static final String TENANT = "tenant";
  private URI filesysURI;
  private Configuration conf;

  @Before
  public void setup() throws URISyntaxException {
    filesysURI = new URI("swift://" + HOSTNAME + "/");
    conf = new Configuration(true);
    setInstanceVal(conf, HOSTNAME, DOT_AUTH_URL, AUTH_URL);
    setInstanceVal(conf, HOSTNAME, DOT_USERNAME, USER);
    setInstanceVal(conf, HOSTNAME, DOT_PASSWORD, PASS);
  }

  private void setInstanceVal(Configuration conf,
                              String host,
                              String key,
                              String val) {
    String instance = RestClientBindings.buildSwiftInstancePrefix(host);
    String confkey = instance
                     + key;
    conf.set(confkey, val);
  }

  @Test
  public void testPrefixBuilder() throws Throwable {
    String built = RestClientBindings.buildSwiftInstancePrefix(HOSTNAME);
    assertEquals("fs.swift.service.hostname", built);
  }

  @Test
  public void testBindAgainstConf() throws Exception {
    Properties props = RestClientBindings.bind(filesysURI, conf);
    assertPropertyEquals(props, SWIFT_AUTH_PROPERTY, AUTH_URL);
    assertPropertyEquals(props, SWIFT_USERNAME_PROPERTY, USER);
    assertPropertyEquals(props, SWIFT_PASSWORD_PROPERTY, PASS);

    assertPropertyEquals(props, SWIFT_TENANT_PROPERTY, null);
    assertPropertyEquals(props, SWIFT_REGION_PROPERTY, null);
    assertPropertyEquals(props, SWIFT_HTTP_PORT_PROPERTY, null);
    assertPropertyEquals(props, SWIFT_HTTPS_PORT_PROPERTY, null);
  }

  @Test(expected = SwiftConfigurationException.class)
  public void testBindAgainstConfMissingInstance() throws Exception {
    Configuration badConf = new Configuration();
    RestClientBindings.bind(filesysURI, badConf );
  }


  @Test(expected = SwiftConfigurationException.class)
  public void testBindAgainstConfIncompleteInstance() throws Exception {
    String instance = RestClientBindings.buildSwiftInstancePrefix(HOSTNAME);
    conf.unset(instance+DOT_PASSWORD);
    RestClientBindings.bind(filesysURI, conf );
  }


}
