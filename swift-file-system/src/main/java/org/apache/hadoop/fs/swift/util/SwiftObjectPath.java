/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.swift.util;

import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * Swift hierarchy mapping
 */
public class SwiftObjectPath {
  private static final Pattern PATH_PART_PATTERN = Pattern.compile(".*/AUTH_\\w*/");

  /**
   * Swift container
   */
  private final String container;

  /**
   * swift object
   */
  private final String object;

  public SwiftObjectPath(String container, String object) {
    this.container = container;
    this.object = object;
  }

  public String getContainer() {
    return container;
  }

  public String getObject() {
    return object;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SwiftObjectPath)) return false;
    final SwiftObjectPath that = (SwiftObjectPath) o;
    return this.toUriPath().equals(that.toUriPath());
  }

  @Override
  public int hashCode() {
    int result = container.hashCode();
    result = 31 * result + object.hashCode();
    return result;
  }

  public String toUriPath() {
    if (container.endsWith("/")) {
      return container + object;
    }
    else if (object.startsWith("/")) {
      return container + object;
    }
    else {
      return container + "/" + object;
    }
  }

  @Override
  public String toString() {
    return toUriPath();
  }

  public static SwiftObjectPath fromPath(URI uri, Path path) {
    final String url = path.toUri().getPath().replaceAll(PATH_PART_PATTERN.pattern(), "");

    return new SwiftObjectPath(uri.getHost(), url);
  }
}
