package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.http.HttpHeaders;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * File system store implementation.
 * Makes REST requests, parses data from responses
 */
public class SwiftNativeFileSystemStore {
  private static final Pattern URI_PATTERN = Pattern.compile("\"\\S+?\"");
  private static final String PATTERN = "EEE, d MMM yyyy hh:mm:ss zzz";
  private URI uri;
  private SwiftRestClient swiftRestClient;

  /**
   * Initalize the filesystem store -this creates the REST client binding.
   * @param fsURI URI of the filesystem, which is used to map to the filesystem-specific
   * options in the configuration file
   * @param configuration configuration
   * @throws IOException on any failure.
   */
  public void initialize(URI fsURI, Configuration configuration) throws IOException {
    this.uri = fsURI;
    this.swiftRestClient = SwiftRestClient.getInstance(fsURI, configuration);
  }

  @Override
  public String toString() {
    return "SwiftNativeFileSystemStore with "
           + swiftRestClient;
  }

  /**
   * Upload a file
   * @param path destination path in the swift filesystem
   * @param inputStream input data
   * @param length length of the data
   * @throws IOException on a problem
   */
  public void uploadFile(Path path, InputStream inputStream, long length) throws IOException {
    swiftRestClient.upload(toObjectPath(path), inputStream, length);
  }

  /**
   * Upload part of a larger file.
   * @param path destination path
   * @param partNumber item number in the path
   * @param inputStream input data
   * @param length length of the data
   * @throws IOException on a problem
   */
  public void uploadFilePart(Path path, int partNumber, InputStream inputStream, long length) throws IOException {
    String stringPath = path.toUri().toString();
    if (stringPath.endsWith("/")) {
      stringPath = stringPath.concat(String.valueOf(partNumber));
    } else {
      stringPath = stringPath.concat("/").concat(String.valueOf(partNumber));
    }

    swiftRestClient.upload(new SwiftObjectPath(uri.getHost(), stringPath), inputStream, length);
  }

  /**
   * Tell the Swift server to expect a multi-part upload by submitting
   * a 0-byte file with the X-Object-Manifest header
   * @param path path of final final
   * @throws IOException
   */
  public void createManifestForPartUpload(Path path) throws IOException {
    String pathString = toObjectPath(path).toString();
    if (!pathString.endsWith("/")) {
      pathString = pathString.concat("/");
    }
    if (pathString.startsWith("/")) {
      pathString = pathString.substring(1);
    }

    swiftRestClient.upload(toObjectPath(path),
                           new ByteArrayInputStream(new byte[0]),
                           0,
                           new Header(SwiftProtocolConstants.X_OBJECT_MANIFEST, pathString));
  }

  /**
   * Get the metadata of an object
   * @param path path
   * @return file metadata. -or null if no headers were received back from the server.
   * @throws IOException on a problem
   * @throws FileNotFoundException if there is nothing at the end
   */
  public FileStatus getObjectMetadata(Path path) throws IOException {
    final Header[] headers;
    headers = swiftRestClient.headRequest(toObjectPath(path));
    //no headers is treated as a missing file
    if (headers.length == 0) {
      throw new FileNotFoundException("Not Found " + path.toUri());
    }

    boolean isDir = false;
    long length = 0;
    long lastModified = System.currentTimeMillis();
    for (Header header : headers) {
      String headerName = header.getName();
      if (headerName.equals(SwiftProtocolConstants.X_CONTAINER_OBJECT_COUNT) ||
          headerName.equals(SwiftProtocolConstants.X_CONTAINER_BYTES_USED)) {
        length = 0;
        isDir = true;
      }
      if (HttpHeaders.CONTENT_LENGTH.equals(headerName)) {
        length = Long.parseLong(header.getValue());
      }
      if (HttpHeaders.LAST_MODIFIED.equals(headerName)) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN);
        try {
          lastModified = simpleDateFormat.parse(header.getValue()).getTime();
        } catch (ParseException e) {
          throw new SwiftException("Failed to parse " + header.toString(), e);
        }
      }
    }

    final Path correctSwiftPath;
    try {
      correctSwiftPath = getCorrectSwiftPath(path);
    } catch (URISyntaxException e) {
      throw new SwiftException("Specified path " + path + " is incorrect", e);
    }
    return new FileStatus(length, isDir, 0, 0L, lastModified, correctSwiftPath);
  }

  public InputStream getObject(Path path) throws IOException {
    return swiftRestClient.getDataAsInputStream(toObjectPath(path));
  }

  /**
   * Get the input stream starting from a specific point.
   * @param path path to object
   * @param byteRangeStart starting point
   * @param length no. of bytes
   * @return an input stream that must be closed
   * @throws IOException IO problems
   */
  public InputStream getObject(Path path, long byteRangeStart, long length)
    throws IOException {
    return swiftRestClient.getDataAsInputStream(
      toObjectPath(path), byteRangeStart, length);
  }

  public FileStatus[] listSubPaths(Path path) throws IOException {
    final Collection<FileStatus> fileStatuses;
    fileStatuses = listDirectory(toObjectPath(path));

    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  public void createDirectory(Path path) throws IOException {
    swiftRestClient.putRequest(toObjectPath(path));
  }

  private SwiftObjectPath toObjectPath(Path path) throws
                                                  SwiftConfigurationException {
    return SwiftObjectPath.fromPath(uri, path);
  }

  public List<URI> getObjectLocation(Path path) throws IOException {
    final byte[] objectLocation;
    objectLocation = swiftRestClient.getObjectLocation(
      toObjectPath(path));
    return extractUris(new String(objectLocation));
  }

  /**
   * deletes object from Swift
   *
   * @param path path to delete
   * @return true if the path was deleted by this specific operation.
   * @throws IOException on a failure
   */
  public boolean deleteObject(Path path) throws IOException {
    return swiftRestClient.delete(toObjectPath(path));
  }

  /**
   * Checks if specified path exists
   *
   * @param path to check
   * @return true - path exists, false otherwise
   */
  public boolean objectExists(Path path) throws IOException {
    return !listDirectory(toObjectPath(path)).isEmpty();
  }

  /**
   * Rename through copy-and-delete. this is clearly very inefficient, and
   * is a consequence of the classic Swift filesystem using the path as the hash
   * into the Distributed Hash Table, "the ring" of filenames.
   * 
   * Because of the nature of the operation, it is not atomic.
   * @param src source file/dir
   * @param dst destination
   * @return true if the entire rename was successful.
   * @throws IOException
   */
  public boolean renameDirectory(Path src, Path dst) throws IOException {
    final FileStatus srcMetadata = getObjectMetadata(src);
    final FileStatus dstMetadata = getObjectMetadata(dst);

    boolean srcExists = srcMetadata != null;
    boolean destExists = dstMetadata != null;
    if (srcExists && !srcMetadata.isDir()) {
      //source exists and is not a directory

      if (destExists && !dstMetadata.isDir()) {
        //if the dest file exists: fail
        throw new SwiftException("A file already exists at the destination: " + dst);
      }

      //calculate the destination
      SwiftObjectPath destPath;
      if (destExists && dstMetadata.isDir()) {
        //destination id a directory: create a path from the destination
        //and the source name

        //REVISIT: this uses dst.getParent(), and not dst itself. Why?
        destPath = toObjectPath(new Path(dst.getParent(),
                 src.getName()));
      } else {
        //destination is a simple file
        destPath = toObjectPath(dst);
      }
      //do the copy
      return swiftRestClient.copyObject(toObjectPath(src),
                                        destPath);
    } else {

      //here the source exists and is a directory
      List<FileStatus> fileStatuses =
        listDirectory(toObjectPath(src.getParent()));
      List<FileStatus> dstPath =
        listDirectory(toObjectPath(dst.getParent()));

      if (dstPath.size() == 1 && !dstPath.get(0).isDir()) {
        throw new SwiftException("Cannot rename to: " + dst.toString());
      }

      boolean result = true;
      for (FileStatus fileStatus : fileStatuses) {
        if (!fileStatus.isDir()) {
          result &=
            swiftRestClient.copyObject(toObjectPath(fileStatus.getPath()),
                                       toObjectPath(dst));

          swiftRestClient.delete(toObjectPath(fileStatus.getPath()));
        }
      }

      return result;
    }
  }

  /**
   * List a directory
   * @param path path to list
   * @return the filestats of all the entities in the directory -or
   * an empty list if no objects were found listed under that prefix
   * @throws IOException IO problems
   */
  private List<FileStatus> listDirectory(SwiftObjectPath path) throws IOException {
    String pathURI = path.toUriPath();
    if (!pathURI.endsWith(Path.SEPARATOR)) {
      pathURI += Path.SEPARATOR;
    }

    final byte[] bytes;
    try {
      bytes = swiftRestClient.findObjectsByPrefix(path);
    } catch (FileNotFoundException e) {
      return Collections.emptyList();
    }

    final StringTokenizer tokenizer = new StringTokenizer(new String(bytes), "\n");
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();

    while (tokenizer.hasMoreTokens()) {
      String pathInSwift = tokenizer.nextToken();
      if (!pathInSwift.startsWith("/")) {
        pathInSwift = "/".concat(pathInSwift);
      }
      final FileStatus metadata = getObjectMetadata(new Path(pathInSwift));
      if (metadata != null) {
        files.add(metadata);
      }
    }

    return files;
  }

  private Path getCorrectSwiftPath(Path path) throws URISyntaxException {
    final URI fullUri = new URI(uri.getScheme(),
                                uri.getAuthority(),
                                path.toUri().getPath(),
                                null,
                                null);

    return new Path(fullUri);
  }

  /**
   * extracts URIs from json
   *
   * @return URIs
   */
  public static List<URI> extractUris(String json) {
    final Matcher matcher = URI_PATTERN.matcher(json);
    final List<URI> result = new ArrayList<URI>();
    while (matcher.find()) {
      final String s = matcher.group();
      final String uri = s.substring(1, s.length() - 1);
      result.add(URI.create(uri));
    }
    return result;
  }
}
