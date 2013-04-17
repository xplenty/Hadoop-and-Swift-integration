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
 * Generates sample data to a destination file, #of lines and a seed
 * for a random no. generator
 */
@Commons
class DataGenerator  extends AbstractDataGenerator{

  int lines
  long seed
  private final Random gaussian, boolRandom, charRandom

  DataGenerator(int lines, long seed) {
    this.lines = lines
    this.seed = seed
    gaussian = new Random(seed)
    boolRandom = new Random(seed)
    charRandom = new Random(seed)
  }

  /**
   * Generate the data and push it to the output stream.
   * @param out an output stream that will stay open
   */
  def generate(DataOutputStream out) {
    log.info("Generating ${lines} lines of data with seed=${seed}")
    1.upto(lines) { line ->
      double g = gaussian.nextGaussian()
      boolean b = boolRandom.nextBoolean()
      char c = (65 + charRandom.nextInt(64))
      String row = "${line},${g},${b},${c}\n"
      log.debug(row)
      out.writeBytes(row)
    }
  }
}
