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

package org.apache.hadoop.fs.swift.integration.generate

import groovy.util.logging.Commons
import org.junit.Test
import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.swift.integration.tools.DataGenerator
import org.apache.hadoop.fs.swift.exceptions.SwiftPathExistsException
import org.apache.hadoop.fs.swift.util.SwiftTestUtils
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem

/**
 * Generate a swift doc
 */
@Commons
class TestGenerateToSwift extends IntegrationTestBase {

  public static final String GENERATED_DATA_DIR = "/data/generated"
  public static final String DATASET_CSV = "dataset.csv"
  public static final String DATASET_CSV_PATH = "/data/generated/dataset.csv"
  

  @Test
  public void testMaybeGenerate() throws Throwable {
    FileSystem fs = getSharedFilesystem()
    Path generatedDir = new Path(GENERATED_DATA_DIR);
    Path generatedData = new Path(generatedDir, DATASET_CSV);
    fs.mkdirs(generatedDir);
    int lines = 100
    try {
      boolean overwrite = true
      FSDataOutputStream out = fs.create(generatedData, overwrite);
      DataGenerator generator = new DataGenerator(lines, 500);
      generator.generate(out)
      out.close();
    } catch (SwiftPathExistsException e) {
      SwiftTestUtils.downgrade("Destination file ${generatedData} exists", e);
    }
    
    //now read it back in
    FSDataInputStream instream = fs.open(generatedData)
    BufferedReader reader = new BufferedReader(new InputStreamReader(instream))
    def linesread = reader.eachLine { line, count ->
      log.info("${count}: " + line)
      count
    }
    reader.close()
    assert lines == linesread
    
  }
}
