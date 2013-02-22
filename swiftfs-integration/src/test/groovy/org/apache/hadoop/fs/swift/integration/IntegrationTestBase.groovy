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
import org.apache.pig.PigServer
import org.apache.pig.impl.util.PropertiesUtil
import org.apache.pig.impl.PigContext
import org.apache.pig.ExecType
import org.apache.pig.data.Tuple
import org.apache.hadoop.fs.FileSystem

@Commons
class IntegrationTestBase extends Assert {
  /**
   * name of the key in the config XML files defining the filesystem to work with
   */
  public static final String KEY_TEST_FS = SwiftTestUtils.TEST_FS_SWIFT;

  protected SwiftNativeFileSystem bindFilesystem() {
    def conf = new Configuration();
    URI serviceURI = getSourceFS(conf);
    SwiftNativeFileSystem fs = new SwiftNativeFileSystem();
    fs.initialize(serviceURI, conf);
    fs
  }
  

  protected FileSystem getSharedFilesystem() {
    def conf = new Configuration();
    URI serviceURI = getSourceFS(conf);
    return FileSystem.get(serviceURI,conf);
  }
  
  

  protected URI getSourceFS(Configuration conf) {
    SwiftTestUtils.getServiceURI(conf)
  }

  protected URI getDestFS(Configuration conf) {
    SwiftTestUtils.getServiceURI(conf)
  }



  protected PigServer createPigServer() {
    Properties properties = PropertiesUtil.loadDefaultProperties()

    PigContext context = new PigContext(ExecType.LOCAL,
                                        properties)
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig
  }

  /**
   * Build the initial parameter map.
   * This includes injecting the source and dest 
   * parameters
   * @return a map
   */
  Map paramMap() {
    def conf = new Configuration();
    URI sourceURI = getSourceFS(conf);
    URI destURI = getDestFS(conf);
    def map = [:]
    map["source"] = sourceURI.toString();
    map["dest"] = destURI.toString();
    map
  }

  String stringify(Tuple t) {
    StringBuilder sb = new StringBuilder("{")
    t.getAll().each { field ->
      sb.append(" \"")
      sb.append(field.toString())
      sb.append("\"")
    }
    sb.append(" }")
    sb.toString()
  }


  protected void dumpTuples(Iterator<Tuple> iterator) {
    iterator.each { ntuple ->
      log.info(stringify(ntuple))
    }
  }

  protected void registerPigResource(PigServer pig, String scriptResource, Map map) {
    InputStream stream = this.getClass().getClassLoader().
        getResourceAsStream(scriptResource)
    if (stream == null) {
      throw new FileNotFoundException("Could not load resource ${scriptResource}")
    }
    pig.registerScript(stream, map, null)
  }
}
