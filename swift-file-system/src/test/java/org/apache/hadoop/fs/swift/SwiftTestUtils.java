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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.junit.internal.AssumptionViolatedException;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Utilities used across test cases
 */
public class SwiftTestUtils {

  private static final Log LOG =
    LogFactory.getLog(SwiftTestUtils.class);

  protected static final String TEST_FS_SWIFT = "test.fs.swift.name";

  /**
   * Get the test URI
   * @param conf configuration
   * @throws SwiftConfigurationException missing parameter or bad URI
   */
  public static URI getServiceURI(Configuration conf) throws
                                                      SwiftConfigurationException {
    String instance = conf.getTrimmed(TEST_FS_SWIFT);
    if (instance == null) {
      throw new SwiftConfigurationException(
        "Missing configuration entry " + TEST_FS_SWIFT);
    }
    try {
      return new URI(instance);
    } catch (URISyntaxException e) {
      throw new SwiftConfigurationException("Bad URI: " + instance);
    }
  }

  public static boolean hasServiceURI(Configuration conf) {
    String instance = conf.getTrimmed(TEST_FS_SWIFT);
    return instance != null;
  }

  /**
   * Assert that a property in the property set matches the expected value
   * @param props property set
   * @param key property name
   * @param expected expected value. If null, the property must not be in the set
   */
  public static void assertPropertyEquals(Properties props,
                                          String key,
                                          String expected) {
    String val = props.getProperty(key);
    if (expected == null) {
      assertNull("Non null property " + key + " = " + val, val);
    }
    else {
      assertEquals("property " + key + " = " + val,
                   expected,
                   val);
    }
  }

  /**
   * Convert a byte to a character for printing. If the
   * byte value is < 32 -and hence unprintable- the byte is
   * returned as a two digit hex value
   * @param b byte
   * @return the printable character string
   */
  public static String toChar(byte b) {
    if (b >= 0x20) {
      return Character.toString((char) b);
    }
    else {
      return String.format("%02x", b);
    }
  }

  public static String toChar(byte[] buffer) {
    StringBuilder builder = new StringBuilder(buffer.length);
    for (byte b:buffer) {
      builder.append(toChar(b));
    }
    return builder.toString();
  }

  public static byte[] toAsciiByteArray(String s) {
    char[] chars = s.toCharArray();
    int len = chars.length;
    byte[] buffer = new byte[len];
    for (int i=0; i<len; i++) {
      buffer[i]=(byte)(chars[i]&0xff);
    }
    return buffer;
  }

  public static void cleanupInTeardown(FileSystem fileSystem,
                                       String cleanupPath) {
    try {
      if (fileSystem != null) {
        fileSystem.delete(new Path(cleanupPath).makeQualified(fileSystem),
                          true);
      }
    } catch (Exception e) {
      LOG.error("Error deleting "+cleanupPath+": " + e, e);
    }
  }

  /**
   * downgrade a failure to a message and a warning, then an
   * exception for the Junit test runner to mark as failed
   * @param log log to print to
   * @param message text medsage
   * @param failure what failed
   * @throws AssumptionViolatedException always
   */
  public static void downgrade(String message, Throwable failure) {
    LOG.warn("Downgrading test " + message, failure);
    AssumptionViolatedException ave =
      new AssumptionViolatedException(message);
    ave.initCause(failure);
    throw ave;
  }

  /**
   * report an overridem test as unsupported
   * @param message message to use in the text
   * @throws AssumptionViolatedException
   */
  public static void unsupported(String message){
    throw new AssumptionViolatedException(message);
  }

  static void assertFileLength(FileSystem fs, Path path, int expected) throws
                                                                        IOException {
    FileStatus status = fs.getFileStatus(path);
    assertEquals("Wrong file length of file " + path + " status: " + status,
                 expected,
                 status.getLen());
  }

  static void assertDirectory(SwiftNativeFileSystem fs, Path path) throws
                                                                    IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    assertFalse("Should be a dir, but is a file: " + fileStatus,
                fileStatus.isFile());
    assertTrue("Should be a dir -but isn't: " + fileStatus,
               fileStatus.isDirectory());
  }

  static void writeTextFile(SwiftNativeFileSystem fs,
                            Path path,
                            String text,
                            boolean overwrite) throws IOException {
    FSDataOutputStream stream = fs.create(path, overwrite);
    stream.write(toAsciiByteArray(text));
    stream.close();
  }

  protected static void assertDeleted(FileSystem fs,
                               Path path,
                               boolean recursive) throws IOException {
    assertTrue(fs.delete(path, recursive));
    assertFalse("failed to delete " + path, fs.exists(path));
  }

  /**
   * Read in "length" bytes, convert to an ascii string
   * @param fs filesystem
   * @param path path to read
   * @param length #of bytes to read.
   * @return the bytes read and converted to a string
   * @throws IOException
   */
  static String readBytesToString(SwiftNativeFileSystem fs,
                                  Path path,
                                  int length) throws IOException {
    FSDataInputStream in = fs.open(path);
    try {
      byte[] buf = new byte[length];
      in.readFully(0, buf);
      return toChar(buf);
    } finally {
      in.close();
    }
  }

  protected static String getDefaultWorkingDirectory() {
    return "/user/" + System.getProperty("user.name");
  }

  static String ls(FileSystem fileSystem, Path path) throws IOException {
    if (path == null) {
      //surfaces when someone calls getParent() on something at the top of the path
      return "/";
    }
    FileStatus[] stats = new FileStatus[0];
    try {
      stats = fileSystem.listStatus(path);
    } catch (FileNotFoundException e) {
      return "ls " + path + " -file not found";
    }
    String pathname = path.toString();
    return dumpStats(pathname, stats);
  }

  static String dumpStats(String pathname, FileStatus[] stats) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    buf.append("ls ").append(pathname).append(": ").append(stats.length)
       .append("\n");
    for (FileStatus stat : stats) {
      buf.append(stat.toString()).append("\n");
    }
    return buf.toString();
  }
}
