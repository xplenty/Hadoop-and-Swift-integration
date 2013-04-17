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

class Duration {
  
  long start_time;
  long end_time;
  long duration;

  Duration() {
    start()
  }

  def start() {
    start_time = System.currentTimeMillis();
  }
  
  def finish() {
    end_time = System.currentTimeMillis()
    duration = end_time - start_time
  }
  
  def getDurationString() {
    long seconds = (long)(duration / 1000)
    long minutes = (long)(seconds/60)
    
    return String.format("%d:%02d:%03d",
            minutes,
            seconds % 60,
            duration % 1000);
  }

  @Override
  String toString() {
    return getDurationString()
  }
}
