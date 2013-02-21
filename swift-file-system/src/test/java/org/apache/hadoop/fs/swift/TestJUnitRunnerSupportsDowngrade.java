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

package org.apache.hadoop.fs.swift;

import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;

public class TestJUnitRunnerSupportsDowngrade {

  @Test
  public void testAssume() throws Throwable {
    Assume.assumeTrue(false);
  }

  @Test
  public void testAssumeNotNull() throws Throwable {
    Assume.assumeNotNull(null,null);
  }

  @Test
  public void testAssumeExplicitSkip() throws Throwable {
    SwiftTestUtils.skip("skipping");
  }

  @Test
  public void testAssumeDowngrade() throws Throwable {
    SwiftTestUtils.downgrade("skipping", new IOException("inner"));
  }

}
