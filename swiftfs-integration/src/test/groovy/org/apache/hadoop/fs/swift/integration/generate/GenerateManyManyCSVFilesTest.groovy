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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants
import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.hadoop.fs.swift.integration.tools.DataGenerator
import org.junit.Test

/**
 * create lots and lots of small cvs files
 */
@Commons
class GenerateManyManyCSVFilesTest extends IntegrationTestBase {


  @Test
  public void testGenerateManyCSV() throws Throwable {
    Path dataDir = new Path(DATASET_MANY_CSV_PATH);
    Configuration conf = new Configuration();
    int partSize = conf.getInt(SwiftProtocolConstants.SWIFT_PARTITION_SIZE,
                               SwiftProtocolConstants.DEFAULT_SWIFT_PARTITION_SIZE);
    int lines = conf.getInt(KEY_TEST_MANY_MANY_LINES, DEFAULT_TEST_MANY_MANY_LINES);
    int files = conf.getInt(KEY_TEST_MANY_MANY_FILES, DEFAULT_TEST_MANY_MANY_FILES);
    log.info("Writing ${lines} lines to $dataDir")

    DataGenerator generator = new DataGenerator(lines, DEFAULT_SEED);
    generateManyFiles(generator, dataDir, files)
  }

  def String filename(int fileindex) {
    String.format("data-%04d.csv", fileindex)
  }

}
