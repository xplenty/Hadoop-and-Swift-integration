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

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.pig.ExecType
import org.apache.pig.PigServer
import org.apache.pig.data.Tuple
import org.apache.pig.impl.PigContext
import org.apache.pig.impl.util.PropertiesUtil
import org.junit.Assert
import org.junit.internal.AssumptionViolatedException

@Commons
class IntegrationTestBase extends Assert implements Keys {


  protected FileSystem bindFilesystem() {
    getSrcFilesystem();
  }


  protected FileSystem getSrcFilesystem() {
    def conf = new Configuration();
    URI serviceURI = getSrcFilesysURI(conf);
    return FileSystem.get(serviceURI, conf);
  }

  protected FileSystem getDestFilesystem() {
    def conf = new Configuration();
    URI serviceURI = getSrcFilesysURI(conf);
    return FileSystem.get(serviceURI, conf);
  }

  /**
   * This method exists to work around  HADOOP-9482.
   * It opens a file with a null permission rather than "the defaults",
   * so skips the checks for fs
   * @param fs
   * @param f
   * @param overwrite
   * @return
   */
  def FSDataOutputStream createFile(FileSystem fs, Path f, boolean overwrite) {
    def bufferSize = fs.getConf().getInt("io.file.buffer.size", 4096)
    def repl = fs.getDefaultReplication(f)
    def blocksize = fs.getDefaultBlockSize(f)
    fs.create(f, null, overwrite, bufferSize, repl, blocksize, null);
  }

  /**
   * Get the test URI
   * @param conf configuration
   * @throws IOException missing parameter or bad URI
   */
  public static URI getServiceURI(Configuration conf, String key) throws
      IOException {
    String instance = conf.get(key);
    if (instance == null) {
      throw new IOException(
          "Missing configuration entry " + key);
    }
    try {
      return new URI(instance);
    } catch (URISyntaxException e) {
      throw new IOException("Bad URI: " + instance);
    }
  }

  protected URI getSrcFilesysURI(Configuration conf) {
    getServiceURI(conf, Keys.KEY_TEST_FS);
  }

  protected URI getDestFilesysURI(Configuration conf) {
    getSrcFilesysURI(conf)
  }

  /**
   * Assume that a configuration option is set
   * @param conf configuration
   * @param key key to look for
   * @throws AssumptionViolatedException -this is converted to a test skip
   */
  protected void assumeSet(Configuration conf, String key) {
    skip(conf.get(key) == null, "Unset option " + key)
  }

  /**
   * skip a test if a condition is set 
   * @param condition condition to test
   * @param message message to use
   * @throws AssumptionViolatedException -this is converted to a test skip
   */
  protected void skip(boolean condition, String message) {
    if (condition) {
      throw new AssumptionViolatedException(message);
    }
  }

  /**
   * Create a pig server
   * @return
   */
  protected PigServer createPigServer() {
    Properties properties = PropertiesUtil.loadDefaultProperties()
    PigContext context = new PigContext(ExecType.LOCAL,
                                        properties)
    PigServer pig = new PigServer(context);
    pig
  }

  /**
   * Build the initial parameter map.
   * This includes injecting the source and dest 
   * parameters
   * @return a map
   */
  Map<String, String> paramMap() {
    def conf = new Configuration();
    URI sourceURI = getSrcFilesysURI(conf);
    URI destURI = getDestFilesysURI(conf);
    def map = [:]
    map["src"] = sourceURI.toString();
    map["dest"] = destURI.toString();
    map["srcfile"] = DATASET_CSV_PATH;
    map["destdir"] = DESTDIR;
    map
  }

  protected void dumpMap(Map map) {
    map.each { k, v ->
      log.info("$k='$v'")
    }
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
