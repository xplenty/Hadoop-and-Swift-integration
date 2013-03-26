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

package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.util.SwiftUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper for input stream
 */
class SwiftNativeInputStream extends FSInputStream {

  private static final Log LOG = LogFactory.getLog(SwiftNativeInputStream.class);
  
  /**
   * Default buffer size 64mb
   */
  private static final long BUFFER_SIZE = 64 * 1024 * 1024;

  /**
   * File nativeStore instance
   */
  private SwiftNativeFileSystemStore nativeStore;

  /**
   * Hadoop statistics. Used to get info about number of reads, writes, etc.
   */
  private FileSystem.Statistics statistics;

  /**
   * Data input stream
   */
  private InputStream in;

  /**
   * File path
   */
  private final Path path;

  /**
   * Current position
   */
  private long pos = 0;

  private long bufferOffset = 0;

  public SwiftNativeInputStream(SwiftNativeFileSystemStore storeNative,
                                FileSystem.Statistics statistics,
                                Path path)
          throws IOException {
    this.nativeStore = storeNative;
    this.statistics = statistics;
    this.in = storeNative.getObject(path);
    this.path = path;
  }

  /**
   * Move to a new position within the file relative to where the pointer is now.
   * Always call from a synchronized clause
   * @param offset offset
   */
  private void incPos(int offset) {
    pos += offset;
    bufferOffset += offset;
    SwiftUtils.trace(LOG, "Inc: pos=%d bufferOffset=%d", pos, bufferOffset);
  }

  /**
   * Update the start of the buffer; always call from a sync'd clause
   * @param seekPos position sought.
   */
  private void updateStartOfBufferPosition(long seekPos) {
    //reset the seek pointer
    pos = seekPos;
    //and put the buffer offset to 0
    bufferOffset = 0;
    SwiftUtils.trace(LOG, "Move: pos=%d bufferOffset=%d", pos, bufferOffset);
  }
  
  @Override
  public synchronized int read() throws IOException {
    int result;
    try {
      result = in.read();
    } catch (IOException e) {
      String msg = "IOException while reading " + path
                   + ": ' +e, attempting to reopen.";
      LOG.info(msg);
      LOG.debug(msg, e);

      seek(pos);
      result = in.read();
    }
    if (result != -1) {
      incPos(1);
    }
    if (statistics != null && result != -1) {
      statistics.incrementBytesRead(1);
    }
    return result;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    int result = -1;
    try {
      result = in.read(b, off, len);
    } catch (IOException e) {
      LOG.info("Received IOException while reading '" + path +
               "', attempting to reopen.");
      seek(pos);
      result = in.read(b, off, len);
    }
    if (result > 0) {
      incPos(result);
      if (statistics != null) {
        statistics.incrementBytesRead(result);
      }
    }

    return result;
  }

  /**
   * close the stream. After this the stream is not usable.
   * This method is thread-safe and idempotent.
   *
   * @throws IOException on IO problems.
   */
  @Override
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      in = null;
    }
  }

  private void chompBytes(long bytes) throws IOException {
    int result;
    for (long i = 0; i < bytes; i++) {
      result = in.read();
      if (result <= 0) {
        throw new SwiftException("Received error code while chomping input");
      }
      incPos(1);
    }
  }

  /**
   * Seek to an offset. If the data is already in the buffer, move to it
   * @param targetPos target position
   * @throws IOException on any problem
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Seek to " + targetPos);
    }
    //there's some special handling of near-local data
    //as the seek can be omitted if it is in/adjacent
    long offset = targetPos - pos;
    if (offset == 0) {
      LOG.debug("seek is no-op");
      return;
    }

    if (bufferOffset + offset < BUFFER_SIZE) {
      //if the seek is in  range of that requested, scan forwards
      //instead of closing and re-opening a new HTTP connection
      SwiftUtils.debug(LOG,
                       "seek is within current stream"
                       + "; pos= %d ; targetPos=%d; "
                       + "offset= %d ; bufferOffset=%d",
                       pos, targetPos, offset, bufferOffset);
      try {
        LOG.debug("chomping ");
        chompBytes(offset);
  
      } catch (IOException e) {
        //this is assumed to be recoverable with a seek -or more likely to fail
        LOG.debug("while chomping ",e);
      }
      if (targetPos - pos == 0) {
        LOG.trace("chomping successful");
        return;
      }
      LOG.trace("chomping failed");
    }

    close();
    in = nativeStore.getObject(path, targetPos, targetPos + BUFFER_SIZE);
    this.pos = targetPos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}