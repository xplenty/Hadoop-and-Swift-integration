package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

/**
 * Unit tests for SwiftObjectPath class.
 */
public class TestSwiftObjectPath {
  private static final Log LOG = LogFactory.getLog(TestSwiftObjectPath.class);

  /**
   * What an endpoint looks like. This is derived from a (valid)
   * rackspace endpoint address
   */
  private static final String ENDPOINT =
    "https://storage101.region1.example.org/v1/MossoCloudFS_9fb40cc0-5c12-11e2-bcfd-0800200c9a66";

  @Test
  public void testParsePath() throws Exception {
    final String pathString = "/home/user/files/file1";
    final Path path = new Path(pathString);
    final URI uri = new URI("http://localhost:35357");
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(uri.getHost(), pathString);

    assertEquals(expected, actual);
  }

  @Test
  public void testParseUrlPath() throws Exception {
    final String pathString = "swift://host1.vm.net:8090/home/user/files/file1";
    final URI uri = new URI(pathString);
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(uri.getHost(), "/home/user/files/file1");

    assertEquals(expected, actual);
  }

  @Test
  public void testParseAuthenticatedUrl() throws Exception {
    final String pathString = "swift://host1.vm.net:8090/v2/AUTH_00345h34l93459y4/home/tom/documents/finance.docx";
    final URI uri = new URI(pathString);
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(uri.getHost(), "/home/tom/documents/finance.docx");

    assertEquals(expected, actual);
  }

  @Test
  public void testConvertToPAth() throws Throwable {
    String initialpath = "/container/dir/file1";
    Path ipath = new Path(
      initialpath);
    SwiftObjectPath objectPath = SwiftObjectPath.fromPath(new URI(initialpath),
                                                          ipath);
    URI endpoint = new URI(ENDPOINT);
    URI uri = SwiftRestClient.pathToURI(objectPath, endpoint);
    LOG.info("Inital Hadoop Path =" + initialpath);
    LOG.info("Merged URI=" + uri);
  }

}
