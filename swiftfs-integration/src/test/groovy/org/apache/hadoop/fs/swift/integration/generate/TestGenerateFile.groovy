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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.hadoop.fs.swift.integration.tools.DataGenerator
import org.junit.Test

/**
 * Generate a swift doc
 */
@Commons
class TestGenerateFile extends IntegrationTestBase {

  @Test
  public void testMaybeGenerate() throws Throwable {
    FileSystem fs = getSrcFilesystem();
    Path generatedData = new Path(DATASET_CSV_PATH);
    Configuration conf = new Configuration();
    int lines = conf.getInt(KEY_TEST_LINES, DEFAULT_TEST_LINES);
    log.info("Writing ${lines} lines to $generatedData via $fs")


    DataGenerator generator = new DataGenerator(lines, DEFAULT_SEED);
    def paths = generateManyFiles(generator, generatedData, 1)

    //now read it back in
    def filepath = paths[0]
    def stat = fs.getFileStatus(filepath)
    log.info("Created file $filepath : $stat" )
    assert stat.len > 0
    
    //next enum files in dir
    
    FSDataInputStream instream = fs.open(filepath)
    BufferedReader reader = new BufferedReader(new InputStreamReader(instream))
    def linesread = reader.eachLine { line, count ->
      log.debug("${count}: " + line)
      count
    }
    reader.close()
    assert lines == linesread
  }

  def String filename(int fileindex) {
    String.format("data-%04d.csv", fileindex)
  }
}
