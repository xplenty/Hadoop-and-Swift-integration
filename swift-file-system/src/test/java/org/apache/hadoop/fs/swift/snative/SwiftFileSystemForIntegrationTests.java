package org.apache.hadoop.fs.swift.snative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/**
 * Class for functional testing huge file upload to Swift FS.
 */
public class SwiftFileSystemForIntegrationTests extends SwiftNativeFileSystem {
  private SwiftNativeFileSystemStore store;

  public void initialize(URI swiftUri, Configuration configuration) throws IOException {
    super.initialize(swiftUri, configuration);
    setConf(configuration);
    if (store == null) {
      store = new SwiftNativeFileSystemStore();
    }

    store.initialize(swiftUri, configuration);
  }

  @Override
  public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite,
                                   int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {

    FSDataOutputStream fsDataOutputStream =
      super.create(file, permission, overwrite, bufferSize, replication,
                   blockSize, progress);
    SwiftNativeOutputStream out =
      (SwiftNativeOutputStream) fsDataOutputStream.getWrappedStream();
    out.setFilePartSize(1024L);
    return fsDataOutputStream;
  }

}
