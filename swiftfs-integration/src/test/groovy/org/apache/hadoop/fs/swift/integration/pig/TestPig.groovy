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

package org.apache.hadoop.fs.swift.integration.pig

import groovy.util.logging.Commons
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.pig.PigServer
import org.apache.pig.data.Tuple
import org.junit.Test

@Commons
public class TestPig extends IntegrationTestBase {

  @Test
  public void testCreateServerInstance() throws Throwable {
    PigServer pig = createPigServer()
  }

  @Test
  public void testLoadGeneratedData() throws Throwable {
    FileSystem fs = getSrcFilesystem();
    skip(!fs.exists(new Path(DATASET_CSV_PATH)),
        "No test data");
    PigServer pig = createPigServer()
    def paramMap = paramMap()
    dumpMap(paramMap)
    //rm the dest dir
    FileSystem destFS = getDestFilesystem();
    destFS.delete(new Path(DESTDIR), true)
    registerPigResource(pig, "pig/loadgenerated.pig", paramMap)
    Iterator<Tuple> iterator = pig.openIterator("result")
    dumpTuples(iterator)
  }


}
