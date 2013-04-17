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
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.hadoop.fs.swift.integration.tools.ByteGenerator
import org.apache.hadoop.fs.swift.integration.tools.Duration
import org.junit.Test

/**
 * This is an action to create a massive set of new files from 
 * a JUnit test case. This isn't the optimal way to do so
 * (hint: an MR job), but it means it can be 
 * done from a JUnit test where time can be measured easily
 */
@Commons
class GenerateMassiveFilesTest extends IntegrationTestBase {


  @Test
  public void testGenerateKilobytes() throws Throwable {
    FileSystem fs = getSrcFilesystem();
    Path dataDir = new Path(DATASET_MASSIVE_PATH);
    fs.mkdirs(dataDir);
    Configuration conf = new Configuration();
    int kb = conf.getInt(KEY_TEST_FILESIZE_KB, DEFAULT_TEST_FILESIZE_KB);
    boolean overwrite = true
    fs.delete(dataDir, true);
    int fileindex = 0
    Path dataFile = new Path(dataDir, filename(fileindex))
    FSDataOutputStream out = createFile(fs, dataFile, overwrite);
    ByteGenerator generator = new ByteGenerator(kb, DEFAULT_SEED);
    Duration writeTime = new Duration()
    Duration totalTime = new Duration()
    generator.generate(out)
    writeTime.finish()
    Duration closeTime = new Duration();
    out.close();
    closeTime.finish()
    totalTime.finish()

    log.info("Total time = $totalTime; write time =$writeTime; close time = $closeTime")
    assertTrue(fs.exists(dataFile));
    def status = fs.getFileStatus(dataFile)
    assertEquals(kb * 1024, status.len)
  }

  def String filename(int fileindex) {
    String.format("data-%04d.bin", fileindex)
  }

  @Test
  public void testReadKilobytes() throws Throwable {
    FileSystem fs = getSrcFilesystem();
    Path dataDir = new Path(DATASET_MASSIVE_PATH);
    Path dataFile = new Path(dataDir, filename(0))
    skip(!fs.exists(dataFile), "No test data");
    Configuration conf = new Configuration();
    int kb = conf.getInt(KEY_TEST_FILESIZE_KB, DEFAULT_TEST_FILESIZE_KB);
    byte[] buffer = new byte[1024];
    ByteGenerator generator = new ByteGenerator(1, DEFAULT_SEED);
    byte[] expected = generator.kilobyte
    FSDataInputStream instream = fs.open(dataFile);
    Duration totalTime = new Duration()
    //do a read and check of the first KB only, or this test case takes forever
    instream.readFully(buffer);
    assertArrayEquals(expected, buffer);
    2.upto(kb) {
      instream.readFully(buffer);
    }
    totalTime.finish()
    log.info("Total time = $totalTime")
  }
}
