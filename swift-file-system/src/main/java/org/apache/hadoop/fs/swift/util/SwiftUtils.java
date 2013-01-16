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

package org.apache.hadoop.fs.swift.util;

public final class SwiftUtils {

  /**
   *
   * Join two (non null) paths, inserting a forward slash between them
   * if needed
   * @param path1 first path
   * @param path2 second path
   * @return the combined path
   */
  public static String joinPaths(String path1, String path2) {
    StringBuilder result =
      new StringBuilder(path1.length() + path2.length() + 1);
    result.append(path1);
    boolean insertSlash = true;
    if (path1.endsWith("/")) {
      insertSlash = false;
    }
    else if (path2.startsWith("/")) {
      insertSlash = false;
    }
    if (insertSlash) {
      result.append("/");
    }
    result.append(path2);
    return result.toString();
  }
}
