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

package org.apache.hadoop.fs.swift.integration.tools

import groovy.util.logging.Commons

/**
 * Generates sample data to a destination file,
 * bytes in a given range with no other values
 */
@Commons
class ByteGenerator extends AbstractDataGenerator {

  int kilobytes
  byte[] kilobyte
  long seed
  private final Random random

  ByteGenerator(int kilobytes, long seed) {
    this.kilobytes = kilobytes
    this.seed = seed
    random = new Random(seed)
    kilobyte = new byte[1024]
    random.nextBytes(kilobyte);
  }

  /**
   * Generate the data and push it to the output stream.
   * @param out an output stream that will stay open
   */
  def generate(DataOutputStream out) {
    
    log.info("Generating ${kilobytes} KB of data")
    1.upto(kilobytes) { out.write(kilobyte) }
  }
}
