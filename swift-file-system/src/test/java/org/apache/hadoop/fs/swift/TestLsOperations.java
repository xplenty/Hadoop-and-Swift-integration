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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLsOperations extends SwiftFileSystemBaseTest {

  private Path[] testDirs;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    testDirs = new Path[]{
      path("/test/hadoop/a"),
      path("/test/hadoop/b"),
      path("/test/hadoop/c/1"),
    };
    assertPathDoesNotExist("test directory", testDirs[0]);
    for (Path path : testDirs) {
      mkdirs(path);
    }
  }

  @Test
  public void testListLevelTest() throws Exception {
    LOG.info("===============================================================");
    FileStatus[] paths = fs.listStatus(path("/test"));
    LOG.info("===============================================================");
    assertEquals(SwiftTestUtils.dumpStats("/test", paths), 1, paths.length);
    assertEquals(path("/test/hadoop"), paths[0].getPath());
  }

  @Test
  public void testListLevelTestHadoop() throws Exception {
    FileStatus[] paths;
    LOG.info("===============================================================");
    paths = fs.listStatus(path("/test/hadoop"));
    LOG.info("===============================================================");
    String stats = SwiftTestUtils.dumpStats("/test/hadoop", paths);
    assertEquals(stats, 3, paths.length);
    assertEquals(stats, path("/test/hadoop/a"), paths[0].getPath());
    assertEquals(stats, path("/test/hadoop/b"), paths[1].getPath());
    assertEquals(stats, path("/test/hadoop/c"), paths[2].getPath());

  }

  @Test
  public void testListStatusEmptyDirectory() throws Exception {
    FileStatus[] paths;
    LOG.info("===============================================================");
    paths = fs.listStatus(path("/test/hadoop/a"));
    LOG.info("===============================================================");
    assertEquals(SwiftTestUtils.dumpStats("/test/hadoop/a", paths), 0,
                 paths.length);
  }

  @Test
  public void testListStatusFile() throws Exception {
    Path file = path("/test/filename");
    createFile(file);
    LOG.info("===============================================================");
    FileStatus[] paths = fs.listStatus(file);
    LOG.info("===============================================================");
    assertEquals(SwiftTestUtils.dumpStats("/test/", paths), 1,
                 paths.length);
  }

}
