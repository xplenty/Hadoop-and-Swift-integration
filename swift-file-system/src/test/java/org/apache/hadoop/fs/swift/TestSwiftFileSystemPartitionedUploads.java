package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftFileSystemForFunctionalTests;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * these tests currently are unit tests, but will be
 * moved to functional/integration tests
 */
public class TestSwiftFileSystemPartitionedUploads {
  private static final Log LOG =
    LogFactory.getLog(TestSwiftFileSystemPartitionedUploads.class);

  private URI uri;
  private Configuration conf;
  protected SwiftFileSystemForFunctionalTests fs;

  @Before
  public void setUp() throws Exception {
    uri = getFilesystemURI();
    conf = new Configuration();
    //patch the configuration with the factory of the new driver


    SwiftFileSystemForFunctionalTests swiftFS = new SwiftFileSystemForFunctionalTests();
    swiftFS.setPartitionSize(1024L);
    fs = swiftFS;
    fs.initialize(uri, conf);
  }


  @After
  public void tearDown() throws Exception {
    SwiftTestUtils.cleanupInTeardown(fs, "/test");
  }

  protected URI getFilesystemURI() throws URISyntaxException, IOException {
    return SwiftTestUtils.getServiceURI(new Configuration());
  }

  /**
   * tests functionality for big files ( > 5Gb) upload
   */
  @Test
  public void testFilePartUpload() throws IOException, URISyntaxException {

    final Path path = new Path("/test/huge-file");

    int len = 4096;
    final byte[] src = SwiftTestUtils.dataset(len,32,144);
    FSDataOutputStream out = fs.create(path, false,
                                       fs.getConf()
                                         .getInt("io.file.buffer.size",
                                                 4096),
                                       (short) 1,
                                       1024);
    assertEquals("wrong number of partitons written",
                 0, fs.getPartitionsWritten(out));
    //write first half
    out.write(src, 0, len/2);
    assertEquals("wrong number of partitons written",
                 1, fs.getPartitionsWritten(out));
    //write second half
    out.write(src, len / 2, len / 2);
    assertEquals("wrong number of partitons written",
                 2, fs.getPartitionsWritten(out));
    out.close();
    assertEquals("wrong number of partitons written",
                 3, fs.getPartitionsWritten(out));

    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", len, fs.getFileStatus(path).getLen());

    FSDataInputStream in = fs.open(path);
    byte[] dest = new byte[len];
    in.readFully(0, dest);
    in.close();

    SwiftTestUtils.compareByteArrays(src, dest, len);

  }

  /**
   * test on concurrent file system changes
   */
  @Test(expected = FileNotFoundException.class)
  public void raceConditionOnDirDeleteTest() throws IOException, URISyntaxException, InterruptedException {
    final SwiftNativeFileSystem fileSystem = fs;

    final String message = "message";
    final Path fileToRead = new Path("/home/huge/files/many-files/file");
    final ExecutorService executorService = Executors.newFixedThreadPool(2);
    fileSystem.create(new Path("/home/huge/file/test/file1"));
    fileSystem.create(new Path("/home/huge/documents/doc1"));
    fileSystem.create(new Path("/home/huge/pictures/picture"));

    executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          fileSystem.delete(new Path("/home/huge"), true);
        } catch (IOException e) {
          throw new RuntimeException("test failed", e);
        }
      }
    });
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          final FSDataOutputStream outputStream = fileSystem.create(fileToRead);
          outputStream.write(message.getBytes());
          outputStream.close();
        } catch (IOException e) {
          throw new RuntimeException("test failed", e);
        }
      }
    });

    executorService.awaitTermination(2, TimeUnit.MINUTES);

    fileSystem.open(fileToRead);

  }

  @Test
  public void testRenameDirWithSubDis() throws IOException {
    final SwiftNativeFileSystem fileSystem = fs;

    final String message = "message";
    final Path filePath = new Path("/home/user/documents/file.txt");
    final Path newFilePath = new Path("/home/user/files/file.txt");

    final FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();

    fileSystem.rename(filePath, newFilePath);

    final FSDataInputStream inputStream = fileSystem.open(newFilePath);
    final byte[] data = new byte[20];
    final int read = inputStream.read(data);

    assertEquals(message.length(), read);
    assertEquals(message, new String(data, 0, read));
  }

}
