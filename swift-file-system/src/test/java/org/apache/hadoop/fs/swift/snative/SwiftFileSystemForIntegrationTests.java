package org.apache.hadoop.fs.swift.snative;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * Class for functional testing huge file upload to Swift FS.
 */
public class SwiftFileSystemForIntegrationTests extends SwiftNativeFileSystem {

  private long partitionSize;

  @Override
  public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite,
                                   int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {

    FSDataOutputStream fsDataOutputStream =
      super.create(file, permission, overwrite, bufferSize, replication,
                   blockSize, progress);
    SwiftNativeOutputStream out =
      (SwiftNativeOutputStream) fsDataOutputStream.getWrappedStream();
    partitionSize = 1024L;
    out.setFilePartSize(partitionSize);
    return fsDataOutputStream;
  }


  public long getPartitionSize() {
    return partitionSize;
  }

  public void setPartitionSize(long partitionSize) {
    this.partitionSize = partitionSize;
  }
}
