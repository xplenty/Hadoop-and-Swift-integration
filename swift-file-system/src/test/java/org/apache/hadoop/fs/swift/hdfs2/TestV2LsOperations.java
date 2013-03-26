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

package org.apache.hadoop.fs.swift.hdfs2;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.SwiftFileSystemBaseTest;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.skip;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.touch;

public class TestV2LsOperations extends SwiftFileSystemBaseTest {

  private Path[] testDirs;

  /**
   * Setup creates dirs under test/hadoop
   * @throws Exception
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    //delete the test directory
    Path test = path("/test");
    fs.delete(test, true);
    mkdirs(test);
  }

  /**
   * Create subdirectories and files under test/ for those tests 
   * that want them. Doing so adds overhead to setup and teardown,
   * so should only be done for those tests that need them.
   * @throws IOException on an IO problem
   */
  private void createTestSubdirs() throws IOException {
    testDirs = new Path[]{
      path("/test/hadoop/a"),
      path("/test/hadoop/b"),
      path("/test/hadoop/c/1"),
    };
    assertPathDoesNotExist("test directory setup", testDirs[0]);
    for (Path path : testDirs) {
      mkdirs(path);
    }
  }


  public static void assertListFilesFinds(FileSystem fs, Path dir,
                                          Path subdir) throws IOException {
    skip("Hadoop 2 only");
/*    RemoteIterator<LocatedFileStatus> iterator =
      fs.listFiles(dir, true);
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    while (iterator.hasNext()) {
      LocatedFileStatus next = iterator.next();
      builder.append(next.toString()).append('\n');
      if (next.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
               + " not found in directory " + dir + ":" + builder,
               found);*/
    
  }

  @Test
  public void testListFilesRootDir() throws Throwable {
    createTestSubdirs();
    Path dir = path("/");
    Path child = new Path(dir, "test");
    assertListFilesFinds(fs, dir, child);
  }

  
}
