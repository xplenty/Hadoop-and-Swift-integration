package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Tests for NativeSwiftFS using either the in-memory Swift emulator
 * or the real client
 */
public abstract class NativeSwiftFileSystemContractBaseTest
  extends FileSystemContractBaseTest {

  private static final Log LOG = LogFactory
    .getLog(NativeSwiftFileSystemContractBaseTest.class);

  @Override
  protected void setUp() throws Exception {
    final URI uri = getFilesystemURI();
    final Configuration conf = new Configuration();

    SwiftNativeFileSystem swiftFS = createSwiftFS();
    fs = swiftFS;
    fs.initialize(uri, conf);
    super.setUp();
  }

  /**
   * Get the URI of this filesystem
   * @return a filesystem URI
   * @throws URISyntaxException Any URI parse failure
   * @throws IOException other problems
   */
  protected abstract URI getFilesystemURI()
    throws URISyntaxException, IOException;

  /**
   * Create a basic SwiftFS. This can be done differently for
   * the different implementations (memory vs. live)
   * @throws IOException
   */
  protected abstract SwiftNativeFileSystem createSwiftFS() throws IOException;

  @Override
  public void testMkdirsWithUmask() {
    //overriding to disable
  }

  public void testHasURI() throws Throwable {
    assertNotNull(fs.getUri());
  }
  
  public void testCreateFile() throws Exception {
    final Path f = new Path("/home/user");
    final FSDataOutputStream fsDataOutputStream = fs.create(f);
    fsDataOutputStream.close();
    assertTrue(fs.exists(f));
  }

  public void testDeleteFile() throws IOException {
    final Path f = new Path("/home/user");
    final FSDataOutputStream fsDataOutputStream = fs.create(f);
    fsDataOutputStream.close();

    assertTrue(fs.exists(f));

    fs.delete(f, true);
    final boolean exists = fs.exists(f);
    assertFalse(exists);
  }

  public void testWriteReadFile() throws Exception {
    final Path f = new Path("/home/user");
    final FSDataOutputStream fsDataOutputStream = fs.create(f);
    final String message = "Test string";
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();

    assertTrue(fs.exists(f));
    final FSDataInputStream open = fs.open(f);
    final byte[] bytes = new byte[512];
    final int read = open.read(bytes);
    final byte[] buffer = new byte[read];
    System.arraycopy(bytes, 0, buffer, 0, read);
    assertEquals(message, new String(buffer));
  }

  public void testRenameFile() throws Exception {
    final Path old = new Path("/home/user/file");
    final Path newPath = new Path("/home/bob/file");
    final FSDataOutputStream fsDataOutputStream = fs.create(old);
    final byte[] message = "Some data".getBytes();
    fsDataOutputStream.write(message);
    fsDataOutputStream.close();

    assertTrue(fs.exists(old));
    assertTrue(fs.rename(old, newPath));
    assertTrue(fs.exists(newPath));

    final FSDataInputStream open = fs.open(newPath);
    final byte[] bytes = new byte[512];
    final int read = open.read(bytes);
    final byte[] buffer = new byte[read];
    System.arraycopy(bytes, 0, buffer, 0, read);
    assertEquals(new String(message), new String(buffer));
  }

  public void testRenameDirectory() throws Exception {
    final Path old = new Path("/data/logs");
    final Path newPath = new Path("/var/logs");
    fs.mkdirs(old);

    assertTrue(fs.exists(old));
    assertTrue(fs.rename(old, newPath));
    assertTrue(fs.exists(newPath));
  }

  public void testRenameTheSameDirectory() throws Exception {
    final Path old = new Path("/usr/data");
    fs.mkdirs(old);

    assertTrue(fs.exists(old));
    assertFalse(fs.rename(old, old));
    assertTrue(fs.exists(old));
  }
}
