/*
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

package org.apache.hadoop.fs.swift.http;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.protocol.DefaultProtocolSocketFactory;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.auth.ApiKeyAuthenticationRequest;
import org.apache.hadoop.fs.swift.auth.ApiKeyCredentials;
import org.apache.hadoop.fs.swift.auth.AuthenticationRequest;
import org.apache.hadoop.fs.swift.auth.AuthenticationRequestWrapper;
import org.apache.hadoop.fs.swift.auth.AuthenticationResponse;
import org.apache.hadoop.fs.swift.auth.AuthenticationWrapper;
import org.apache.hadoop.fs.swift.auth.PasswordAuthenticationRequest;
import org.apache.hadoop.fs.swift.auth.PasswordCredentials;
import org.apache.hadoop.fs.swift.auth.entities.AccessToken;
import org.apache.hadoop.fs.swift.auth.entities.Catalog;
import org.apache.hadoop.fs.swift.auth.entities.Endpoint;
import org.apache.hadoop.fs.swift.exceptions.SwiftBadRequestException;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swift.exceptions.SwiftConnectionException;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.exceptions.SwiftInternalStateException;
import org.apache.hadoop.fs.swift.exceptions.SwiftInvalidResponseException;
import org.apache.hadoop.fs.swift.ssl.EasySSLProtocolSocketFactory;
import org.apache.hadoop.fs.swift.util.JSONUtil;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftUtils;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Properties;

import static org.apache.commons.httpclient.HttpStatus.*;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.*;

/**
 * This implements the client-side of the Swift REST API.
 *
 * The core actions put, get and query data in the Swift object store,
 * after authenticationg the client.
 *
 * <b>Logging:</b>
 *
 * Logging at DEBUG level displays detail about the actions of this
 * client, including HTTP requests and responses.
 * Logging at TRACE level displays the authentication payload -
 * and so will reveal the secrets used to authenticate against
 * the service. It should only be done to track down authentication problems,
 * -and the logs should not be made public.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class SwiftRestClient {
  private static final Log LOG = LogFactory.getLog(SwiftRestClient.class);
  private static final int DEFAULT_RETRY_COUNT = 3;
  private static final int DEFAULT_CONNECT_TIMEOUT = 15000;

  /**
   * Header that says "use newest version" -ensures that
   * the query doesn't pick up older versions by accident
   */
  public static final Header NEWEST =
    new Header(SwiftProtocolConstants.X_NEWEST, "true");

  /**
   * authentication endpoint
   */
  private final URI authUri;

  /**
   * Swift region. Some OpenStack installations has more than one region.
   * In this case user can specify region with which Hadoop will be working
   */
  private final String region;

  /**
   * tenant name
   */
  private final String tenant;

  /**
   * username name
   */
  private final String username;

  /**
   * user password
   */
  private final String password;

  /**
   * user api key
   */
  private final String apiKey;

  /**
   * should use xstorage
   */
  private final AuthenticationMethod authenticationMethod;

  /**
   * The container this client is working with
   */
  private final String container;

  /**
   * Access token (Secret)
   */
  private AccessToken token;
  /**
   * Endpoint for swift operations, obtained after authentication
   */
  private URI endpointURI;
  /**
   * Where objects live
   */
  private URI objectLocationURI;
  private final URI filesystemURI;
  /**
   * The name of the service provider
   */
  private final String serviceProvider;

  /**
   * Should the public swift endpoint be used, rather than the in-cluster one?
   */
  private final boolean usePublicURL;

  /**
   * Number of times to retry a connection
   */
  private final int retryCount;

  /**
   * How long (in milliseconds) should a connection be attempted
   */
  private final int connectTimeout;

  /**
  * the name of a proxy host (can be null, in which case there is no proxy)
   */
  private String proxyHost;
  /**
   * The port of a proxy. This is ignored if {@link #proxyHost} is null
   */
  private int proxyPort;

  /**
   * objects query endpoint. This is synchronized
   * to handle a simultaneous update of all auth data in one
   * go.
   */
  private synchronized URI getEndpointURI() {
    return endpointURI;
  }

  /**
   * object location endpoint
   */
  private synchronized URI getObjectLocationURI() {
    return objectLocationURI;
  }

  /**
   * token for Swift communication
   */
  private synchronized AccessToken getToken() {
    return token;
  }

  /**
   * Setter of authentication and endpoint details.
   * Being synchronized guarantees that all three fields are set up together.
   * It is up to the reader to read all three fields in their own
   * synchronized block to be sure that they are all consistent.
   * @param endpoint endpoint URI
   * @param objectLocation object location URI
   * @param authToken auth token
   */
  private void setAuthDetails(URI endpoint,
                              URI objectLocation,
                              AccessToken authToken) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("setAuth: endpoint=%s; objectURI=%s; token=%s",
                              endpoint, objectLocation, token));
    }
    synchronized (this) {
      endpointURI = endpoint;
      objectLocationURI = objectLocation;
      token = authToken;
    }
  }


  /**
   * Base class for all Swift REST operations
   * @param <M> method
   * @param <R> result
   */
  private static abstract class HttpMethodProcessor<M extends HttpMethod, R> {
    public final M createMethod(String uri) throws IOException {
      final M method = doCreateMethod(uri);
      setup(method);
      return method;
    }

    /**
     * Override it to return some result after method is executed.
     */
    public abstract R extractResult(M method) throws IOException;

    /**
     * Factory method to create a REST method against the given URI
     * @param uri target
     * @return method to invoke
     */
    protected abstract M doCreateMethod(String uri);

    /**
     * Override it to set up method before method is executed.
     */
    protected void setup(M method) throws IOException {
    }

    /**
     * Override point: what are the status codes that this operation supports
     * @return the list of status codes to accept
     */
    protected int[] getAllowedStatusCodes() {
      return new int[] {
        SC_OK,
        SC_CREATED,
        SC_ACCEPTED,
        SC_NO_CONTENT,
        SC_PARTIAL_CONTENT,
      };
    }
  }

  private static abstract class GetMethodProcessor<R> extends HttpMethodProcessor<GetMethod, R> {
    @Override
    protected final GetMethod doCreateMethod(String uri) {
      return new GetMethod(uri);
    }
  }

  private static abstract class PostMethodProcessor<R> extends HttpMethodProcessor<PostMethod, R> {
    @Override
    protected final PostMethod doCreateMethod(String uri) {
      return new PostMethod(uri);
    }
  }

  private static abstract class PutMethodProcessor<R> extends HttpMethodProcessor<PutMethod, R> {
    @Override
    protected final PutMethod doCreateMethod(String uri) {
      return new PutMethod(uri);
    }

    /**
     * Override point: what are the status codes that this operation supports
     * @return the list of status codes to accept
     */
    protected int[] getAllowedStatusCodes() {
      return new int[]{
        SC_OK,
        SC_CREATED,
        SC_NO_CONTENT,
        SC_ACCEPTED,
      };
    }
  }

  /**
   * Copy operation.
   * The only valid response is CREATED
   * @param <R>
   */
  private static abstract class CopyMethodProcessor<R> extends HttpMethodProcessor<CopyMethod, R> {
    @Override
    protected final CopyMethod doCreateMethod(String uri) {
      return new CopyMethod(uri);
    }

    protected int[] getAllowedStatusCodes() {
      return new int[]{
        SC_CREATED
      };
    }
  }

  /**
   * Delete operation
   * @param <R>
   */
  private static abstract class DeleteMethodProcessor<R> extends HttpMethodProcessor<DeleteMethod, R> {
    @Override
    protected final DeleteMethod doCreateMethod(String uri) {
      return new DeleteMethod(uri);
    }

    @Override
    protected int[] getAllowedStatusCodes() {
      return new int[]{
        SC_OK,
        SC_ACCEPTED,
        SC_NO_CONTENT,
        SC_NOT_FOUND
      };
    }
  }

  private static abstract class HeadMethodProcessor<R> extends HttpMethodProcessor<HeadMethod, R> {
    @Override
    protected final HeadMethod doCreateMethod(String uri) {
      return new HeadMethod(uri);
    }
  }


  /**
   * Create a Swift Rest Client instance.
   * @param filesystemURI filesystem URI
   * @param conf The configuration to use to extract the binding
   * @throws SwiftConfigurationException the configuration is not valid for
   * defining a rest client against the service
   */
  private SwiftRestClient(URI filesystemURI,
                          Configuration conf)
      throws SwiftConfigurationException {
    this.filesystemURI = filesystemURI;
    Properties props = RestClientBindings.bind(filesystemURI, conf);
    String stringAuthUri = getOption(props, SWIFT_AUTH_PROPERTY);
    username = getOption(props, SWIFT_USERNAME_PROPERTY);
    password = props.getProperty(SWIFT_PASSWORD_PROPERTY);
    apiKey = props.getProperty(SWIFT_APIKEY_PROPERTY);
    //optional
    region = props.getProperty(SWIFT_REGION_PROPERTY);
    //tenant is optional
    tenant = props.getProperty(SWIFT_TENANT_PROPERTY);
    //service is used for diagnostics
    serviceProvider = props.getProperty(SWIFT_SERVICE_PROPERTY);
    container = props.getProperty(SWIFT_CONTAINER_PROPERTY);
    String isPubProp = props.getProperty(SWIFT_PUBLIC_PROPERTY, "false");
    usePublicURL = "true".equals(isPubProp);
    retryCount = getIntOption(props, SWIFT_RETRY_COUNT, DEFAULT_RETRY_COUNT);
    authenticationMethod = getAuthenticationMethodOption(props, SWIFT_AUTHENTICATION_METHOD_PROPERTY, AuthenticationMethod.keystone);	//Default is Keystone authentication
    connectTimeout = getIntOption(props, SWIFT_CONNECTION_TIMEOUT,
                                  DEFAULT_CONNECT_TIMEOUT);
    
    if (apiKey == null && password == null) {
        throw new SwiftConfigurationException(
          "Configuration for "+ filesystemURI +" must contain either "
          + SWIFT_PASSWORD_PROPERTY + " or "
          + SWIFT_APIKEY_PROPERTY);
    }

    proxyHost = props.getProperty(SWIFT_PROXY_HOST_PROPERTY, null);
    proxyPort = getIntOption(props, SWIFT_PROXY_PORT_PROPERTY, 8080);

    if (LOG.isDebugEnabled()) {
      //everything you need for diagnostics. The password is omitted.
      LOG.debug(String.format(
        "Service={%s} container={%s} uri={%s}"
        + " tenant={%s} user={%s} region={%s}"
        + " publicURL={%b}"
        + " connect timeout={%d}, retry count={%d}",
        serviceProvider,
        container,
        stringAuthUri,
        tenant,
        username,
        region != null ? region : "(none)",
        usePublicURL,
        connectTimeout,
        retryCount));
    }
    try {
      this.authUri = new URI(stringAuthUri);
    } catch (URISyntaxException e) {
      throw new SwiftConfigurationException("The " + SWIFT_AUTH_PROPERTY
                                            + " property was incorrect: "
                                            + stringAuthUri, e);
    }
  }

  /**
   * Get a mandatory configuration option
   * @param props property set
   * @param key key
   * @return value of the configuration
   * @throws SwiftConfigurationException if there was no match for the key
   */
  private static String getOption(Properties props, String key) throws
                                                    SwiftConfigurationException {
    String val = props.getProperty(key);
    if (val == null) {
      throw new SwiftConfigurationException("Undefined property: " + key);
    }
    return val;
  }


  private int getIntOption(Properties props, String key, int def) throws
                                                         SwiftConfigurationException {
    String val = props.getProperty(key, Integer.toString(def));
    try {
      return Integer.decode(val);
    } catch (NumberFormatException e) {
      throw new SwiftConfigurationException("Failed to parse (numeric) value" +
                                        " of property" + key
                                       + " : "+val, e);
    }
  }

  /**
   * Get a AuthenticationMethod option from the property object
   *
   * @param props property object
   * @param key   configuration
   * @param def   default value
   * @return the value in the property file, or the default.
   * @throws SwiftConfigurationException if the property file-supplied
   *                                     value cannot be parsed to an integer
   */
  private AuthenticationMethod getAuthenticationMethodOption(Properties props, String key, AuthenticationMethod def) throws
          SwiftConfigurationException {
    String val = props.getProperty(key, def.toString());
    return AuthenticationMethod.valueOf(val);
  }
  
  /**
   * This is something that needs to be looked at, as it is
   * setting the static state of the http client classes.
   */
  private void registerProtocols(Properties props) throws
                                                   SwiftConfigurationException {
    Protocol.registerProtocol("http", new Protocol("http",
                                                   new DefaultProtocolSocketFactory(),
                                                   getIntOption(props,
                                                     SWIFT_HTTP_PORT_PROPERTY,
                                                     SWIFT_HTTP_PORT)));
    Protocol.registerProtocol("https",
                              new Protocol("https",
                                           (ProtocolSocketFactory) new EasySSLProtocolSocketFactory(),
                                           getIntOption(props,
                                             SWIFT_HTTPS_PORT_PROPERTY,
                                             SWIFT_HTTPS_PORT)));
  }
  /**
   * Makes HTTP GET request to Swift
   *
   * @param path   path to object
   * @param offset offset from file beginning
   * @param length file length
   * @return The input stream -which must be closed afterwards.
   */
  public InputStream getDataAsInputStream(SwiftObjectPath path,
                                          long offset,
                                          long length) throws IOException {
    if (offset < 0) {
      throw new IOException("Invalid offset: " + offset + ".");
    }
    if (length <= 0) {
      throw new IOException("Invalid length: " + length + ".");
    }

    final String range = String.format(SWIFT_RANGE_HEADER_FORMAT_PATTERN,
                                       offset,
                                       offset + length - 1);
    return getDataAsInputStream(path,
                                new Header(HEADER_RANGE, range),
                                SwiftRestClient.NEWEST);
  }

  /**
   * Returns object length
   *
   * @param uri file URI
   * @return object length
   * @throws SwiftException on swift-related issues
   * @throws IOException on network/IO problems
   */
  public long getContentLength(URI uri) throws IOException {
    preRemoteCommand("getContentLength");
    return perform(uri, new HeadMethodProcessor<Long>() {
      @Override
      public Long extractResult(HeadMethod method) throws IOException {
        return method.getResponseContentLength();
      }

      @Override
      protected void setup(HeadMethod method) throws IOException {
        super.setup(method);
        method.addRequestHeader(NEWEST);
      }
    });
  }

  /**
   * Get the length of the remote object
   * @param path object to probe
   * @return the content length
   * @throws IOException on any failure
   */
  public long getContentLength(SwiftObjectPath path) throws IOException {
    return getContentLength(pathToURI(path));
  }

  /**
   * Get the path contents as an input stream.
   * <b>Warning:</b> this input stream must be closed to avoid
   * keeping Http connections open.
   *
   * @param path           path to file
   * @param requestHeaders http headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   * @throws FileNotFoundException if there is nothing at the path
   */
  public InputStream getDataAsInputStream(SwiftObjectPath path,
                                          final Header... requestHeaders)
      throws IOException {
    preRemoteCommand("getDataAsInputStream");
    return doGet(pathToURI(path),
                 requestHeaders);
  }

  /**
   * Returns object location as byte[]
   *
   * @param path           path to file
   * @param requestHeaders http headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   */
  public byte[] getObjectLocation(SwiftObjectPath path,
                                  final Header... requestHeaders) throws IOException {
    preRemoteCommand("getObjectLocation");
    return perform(pathToObjectLocation(path),
                   new GetMethodProcessor<byte[]>() {
                     @Override
                     public byte[] extractResult(GetMethod method) throws
                                                                   IOException {
                       //TODO: remove SC_NO_CONTENT if it depends on Swift versions
                       if (method.getStatusCode() == SC_NOT_FOUND ||
                           method.getStatusCode() == SC_NO_CONTENT ||
                           method.getResponseBodyAsStream() == null) {
                         return null;
                       }
                       final InputStream responseBodyAsStream =
                         method.getResponseBodyAsStream();
                       final byte[] locationData = new byte[1024];

                       return responseBodyAsStream.read(locationData) > 0
                              ? locationData
                              : null;
                     }

                     @Override
                     protected void setup(GetMethod method)
                        throws SwiftInternalStateException {
                       setHeaders(method, requestHeaders);
                     }
                   });
  }

  private URI pathToObjectLocation(SwiftObjectPath path) throws SwiftException {
    URI uri;
    String dataLocationURI = objectLocationURI.toString();
    try {
      if (path.toString().startsWith("/")) {
        dataLocationURI = dataLocationURI.concat(path.toUriPath());
      } else {
        dataLocationURI = dataLocationURI.concat("/").concat(path.toUriPath());
      }

      uri = new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new SwiftException(e);
    }
    return uri;
  }

  /**
   * Find objects under a prefix
   *
   * @param path path prefix
   * @param delimiter delimiter of path, can be null
   * @param requestHeaders optional request headers
   * @return  byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   * @throws FileNotFoundException if nothing is at the end of the URI -that is,
   * the directory is empty
   */
  public byte[] findObjectsByPrefix(SwiftObjectPath path,
                                    String delimiter,
                                    final Header... requestHeaders) throws IOException {

    preRemoteCommand("findObjectsByPrefix");
    if (LOG.isDebugEnabled()) {
      LOG.debug("findObjectsByPrefix path=" + path + " delimiter=" + delimiter);
    }
    String endpoint = getEndpointURI().toString();
    StringBuilder dataLocationURI = new StringBuilder();
    dataLocationURI.append(endpoint);
    String object = path.getObject();
    if (object.startsWith("/")) {
      object = object.substring(1);
    }
    dataLocationURI = dataLocationURI.append("/")
                                     .append(path.getContainer());

    maybeAppendPrefix(dataLocationURI, object);

    if (delimiter != null) {
      dataLocationURI.append("&delimiter=/").append(delimiter);
    }
    return findObjects(dataLocationURI.toString(), requestHeaders);
  }

  private void maybeAppendPrefix(StringBuilder dataLocationURI, String object) {
    if (!object.isEmpty() && !"/".equals(object)) {
      dataLocationURI.append("/?prefix=")
                     .append(object);
    }
  }

  /**
   * Find objects in a directory
   *
   * @param path path prefix
   * @param requestHeaders optional request headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   * @throws FileNotFoundException if nothing is at the end of the URI -that is,
   * the directory is empty
   */
  public byte[] listObjectsInDirectory(SwiftObjectPath path,
                                       final Header... requestHeaders) throws IOException {
    preRemoteCommand("listObjectsInPath");
    if (LOG.isDebugEnabled()) {
      LOG.debug("listObjectsInDirectory path=" + path );
    }
    String endpoint = getEndpointURI().toString();
    StringBuilder dataLocationURI1 = new StringBuilder();
    dataLocationURI1.append(endpoint);
    String object = path.getObject();
    if (object.startsWith("/")) {
      object = object.substring(1);
    }
    if (!object.endsWith("/")) {
      object = object.concat("/");
    }

    dataLocationURI1 = dataLocationURI1.append("/")
                                     .append(path.getContainer());
    maybeAppendPrefix(dataLocationURI1, object);
    StringBuilder dataLocationURI = dataLocationURI1;
    dataLocationURI.append("&delimiter=/");
    return findObjects(dataLocationURI.toString(), requestHeaders);
  }

  /**
   * Find objects in a location
   * @param location URI
   * @param requestHeaders optional request headers
   * @return the body of te response
   * @throws IOException IO problems
   */
  private byte[] findObjects(String location, final Header[] requestHeaders) throws
          IOException {
    preRemoteCommand("findObjects");
    URI uri;
    try {
      uri = new URI(location);
    } catch (URISyntaxException e) {
      throw new SwiftException("Bad URI: " + location, e);
    }

    return perform(uri, new GetMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(GetMethod method) throws IOException {
        if (method.getStatusCode() == SC_NOT_FOUND) {
          //no result
          throw new FileNotFoundException("Not found " + method.getURI());
        }
        return method.getResponseBody();
      }

      @Override
      protected int[] getAllowedStatusCodes() {
        return new int[]{
          SC_OK,
          SC_NOT_FOUND
        };
      }

      @Override
      protected void setup(GetMethod method)
        throws SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Copy an object. This is done by sending a COPY method to the filesystem
   * which is required to handle this WebDAV-level extension to the
   * base HTTP operations.
   * @param src source path
   * @param dst destination path
   * @param headers any headers
   * @return true if the status code was considered successful
   * @throws IOException on IO Faults
   */
  public boolean copyObject(SwiftObjectPath src, final SwiftObjectPath dst, final Header... headers)
    throws IOException {
    preRemoteCommand("copyObject");

    return perform(pathToURI(src), new CopyMethodProcessor<Boolean>() {
      @Override
      public Boolean extractResult(CopyMethod method) throws IOException {
        return true;
      }

      @Override
      protected void setup(CopyMethod method) throws
                                              SwiftInternalStateException {
        setHeaders(method, headers);
        method.addRequestHeader(HEADER_DESTINATION, dst.toUriPath());
      }
    });
  }


  /**
   * Uploads file as Input Stream to Swift
   *
   * @param path           path to Swift
   * @param data           object data
   * @param length         length of data
   * @param requestHeaders http headers
   * @throws IOException on IO Faults
   */
  public void upload(SwiftObjectPath path,
                     final InputStream data,
                     final long length,
                     final Header... requestHeaders)
    throws IOException {
    preRemoteCommand("upload");
    perform(pathToURI(path), new PutMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(PutMethod method) throws IOException {
        return method.getResponseBody();
      }

      @Override
      protected void setup(PutMethod method) throws
                                             SwiftInternalStateException {
        method.setRequestEntity(new InputStreamRequestEntity(data, length));
        setHeaders(method, requestHeaders);
      }
    });
  }


  /**
   * Deletes object from swift.
   * The result is true if this operation did the deletion.
   * @param path           path to file
   * @param requestHeaders http headers
   * @throws IOException on IO Faults
   */
  public boolean delete(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    preRemoteCommand("delete");

    return perform(pathToURI(path), new DeleteMethodProcessor<Boolean>() {
      @Override
      public Boolean extractResult(DeleteMethod method) throws IOException {
        return method.getStatusCode() == SC_NO_CONTENT;
      }

      @Override
      protected void setup(DeleteMethod method) throws
                                                SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Issue a head request
   * @param path path to query
   * @param requestHeaders request header
   * @return the response headers. This may be an empty list
   * @throws IOException IO problems
   * @throws FileNotFoundException if there is nothing at the end
   */
  public Header[] headRequest(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    preRemoteCommand("headRequest");
    return perform(pathToURI(path), new HeadMethodProcessor<Header[]>() {
      @Override
      public Header[] extractResult(HeadMethod method) throws IOException {
        if (method.getStatusCode() == SC_NOT_FOUND) {
          throw new FileNotFoundException("Not Found " + method.getURI());
        }

        return method.getResponseHeaders();
      }

      @Override
      protected void setup(HeadMethod method) throws
                                              SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  public int putRequest(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    preRemoteCommand("putRequest");
    return perform(pathToURI(path), new PutMethodProcessor<Integer>() {

      @Override
      public Integer extractResult(PutMethod method) throws IOException {
        return method.getStatusCode();
      }

      @Override
      protected void setup(PutMethod method) throws
                                             SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Authenticate to Openstack Keystone
   * As well as returning the access token, the member fields {@link #token},
   * {@link #endpointURI} and {@link #objectLocationURI} are set up for re-use.
   *
   * This method is re-entrant -if more than one thread attempts to authenticate
   * neither will block -but the field values with have those of the last caller.
   *
   * <b>Important:</b> if executed at TRACE level then this method will log the
   * JSON payload of the authentication. While this can be invaluable for debugging
   * authentication problems, it can include login information -including
   * the password. Only turn this level of logging on when dealing with
   * authentication problems.
   * @return authenticated access token
   */
  public AccessToken authenticate() throws IOException {
    LOG.debug("started authentication");
    if (authenticationMethod == AuthenticationMethod.swauth)
    	return authenticateXStorage();
    return perform(authUri, new PostMethodProcessor<AccessToken>() {
      @Override
      protected void setup(PostMethod method) throws SwiftException {
        AuthenticationRequest authRequest = null;
        if (password != null) {
          authRequest = new PasswordAuthenticationRequest(tenant,
                                                          new PasswordCredentials(
                                                            username,
                                                            password));
        } else {
          authRequest = new ApiKeyAuthenticationRequest(tenant,
                                                        new ApiKeyCredentials(
                                                          username, apiKey));
        }
        final String data = JSONUtil.toJSON(new AuthenticationRequestWrapper(
          authRequest));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Authenticating with " + authRequest);
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("JSON message: " + "\n" + data);
        }
        method.setRequestEntity(toJsonEntity(data));
      }

      /**
       * specification says any of the 2xxs are OK, so list all
       * the standard ones
       * @return a set of 2XX status codes.
       */
      @Override
      protected int[] getAllowedStatusCodes() {
        return new int[]{
          SC_OK,
          SC_CREATED,
          SC_ACCEPTED,
          SC_NON_AUTHORITATIVE_INFORMATION,
          SC_NO_CONTENT,
          SC_RESET_CONTENT,
          SC_PARTIAL_CONTENT,
          SC_MULTI_STATUS
        };
      }

      @Override
      public AccessToken extractResult(PostMethod method) throws IOException {
        final AuthenticationResponse access =
                JSONUtil.toObject(method.getResponseBodyAsString(),
                                  AuthenticationWrapper.class).getAccess();
        final List<Catalog> serviceCatalog = access.getServiceCatalog();
        //locate the specific service catalog that defines Swift; variations
        //in the name of this add complexity to the search
        boolean catalogMatch = false;
        StringBuilder catList = new StringBuilder();
        StringBuilder regionList = new StringBuilder();

        //these fields are all set together at the end of the operation
        URI endpointURI = null;
        URI objectLocation;
        Endpoint swiftEndpoint = null;
        AccessToken accessToken;

        for (Catalog catalog : serviceCatalog) {
          String name = catalog.getName();
          String type = catalog.getType();
          String descr = String.format("[%s: %s]; ", name, type);
          catList.append(descr);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Catalog entry " + descr);
          }
          if (name.equals(SERVICE_CATALOG_SWIFT)
              || name.equals(SERVICE_CATALOG_CLOUD_FILES)
              || type.equals(SERVICE_CATALOG_OBJECT_STORE)) {
            //swift is found
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found swift catalog as " + name + " => " + type);
            }
            //now go through the endpoints
            for (Endpoint endpoint : catalog.getEndpoints()) {
              String endpointRegion = endpoint.getRegion();
              URI publicURL = endpoint.getPublicURL();
              URI internalURL = endpoint.getInternalURL();
              descr = String.format("[%s =>  %s / %s]; ",
                                    endpointRegion,
                                    publicURL,
                                    internalURL);
              regionList.append(descr);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Endpoint " + descr);
              }
              if (region == null || endpointRegion.equals(region)) {
                endpointURI = usePublicURL  ?publicURL: internalURL;
                swiftEndpoint = endpoint;
                break;
              }
            }
          }
        }
        if (endpointURI == null) {
          String message = "Could not find swift service from auth URL "
                           + authUri
                           + " and region '" + region + "'. "
                           + "Categories: " + catList
                           + ((regionList.length() > 0) ?
                              ("regions: " + regionList)
                                                        : "No regions");
          throw new SwiftInvalidResponseException(message,
                                                  SC_OK,
                                                  "authenticating",
                                                  authUri);

        }


        accessToken = access.getToken();
        String path = SWIFT_OBJECT_AUTH_ENDPOINT
                      + swiftEndpoint.getTenantId();
        String host = endpointURI.getHost();
        try {
          objectLocation = new URI(endpointURI.getScheme(),
                                      null,
                                      host,
                                      endpointURI.getPort(),
                                      path,
                                      null,
                                      null);
        } catch (URISyntaxException e) {
          throw new SwiftException("object endpoint URI is incorrect: "
                                   + endpointURI
                                   + " + " + path,
                                   e);
        }
        setAuthDetails(endpointURI, objectLocation, accessToken);

        if (LOG.isDebugEnabled()) {
          LOG.debug("authenticated against " + endpointURI);
        }
        createDefaultContainer();
        return accessToken;
      }
    });
  }
  
  public AccessToken authenticateXStorage() throws IOException {
	   
	    final PasswordCredentials cred = new PasswordCredentials( username, password);

	    LOG.debug("started authentication");
	    return perform(authUri, new GetMethodProcessor<AccessToken>() {


	      @Override
	      protected void setup(GetMethod method) throws SwiftException {

	    	  for (Header h : getXStorageHeaders(cred))
	    		  method.setRequestHeader(h);
	      }

	      /**
	       * specification says any of the 2xxs are OK, so list all
	       * the standard ones
	       * @return a set of 2XX status codes.
	       */
	      @Override
	      protected int[] getAllowedStatusCodes() {
	        return new int[]{
	                SC_OK,
	                SC_BAD_REQUEST,
	                SC_CREATED,
	                SC_ACCEPTED,
	                SC_NON_AUTHORITATIVE_INFORMATION,
	                SC_NO_CONTENT,
	                SC_RESET_CONTENT,
	                SC_PARTIAL_CONTENT,
	                SC_MULTI_STATUS,
	                SC_UNAUTHORIZED //if request unauthorized, try another method
	        };
	      }

	      @Override
	      public AccessToken extractResult(GetMethod method) throws IOException {
	        //initial check for failure codes leading to authentication failures
	        if (method.getStatusCode() == SC_BAD_REQUEST) {
	          throw new SwiftInvalidResponseException(
	        		  cred.toString(), SC_BAD_REQUEST, "GET", authUri);
	        }
	        Header authHeader = method.getResponseHeader(SwiftProtocolConstants.HEADER_AUTH_KEY);
	        Header storageUrlHeader = method.getResponseHeader(SwiftProtocolConstants.HEADER_WSAUTH_URL_KEY);

	        if (authHeader == null || storageUrlHeader == null)
		          throw new SwiftInvalidResponseException(
		        		  cred.toString(), method.getStatusCode(), "GET", authUri);
	        
	        URI objectLocation;
	        URI endpointURI;
			try { 
				endpointURI= new URI(storageUrlHeader.getValue());
				objectLocation = new URI(storageUrlHeader.getValue());
			} catch (URISyntaxException e) {
		          throw new SwiftInvalidResponseException(
		        		  cred.toString(), method.getStatusCode(), "GET", authUri);				
			}
	        AccessToken accessToken = new AccessToken();
	        accessToken.setId(authHeader.getValue());
	        
	        setAuthDetails(endpointURI, objectLocation, accessToken);

	        if (LOG.isDebugEnabled()) {
	          LOG.debug("authenticated against " + endpointURI);
	        }
	        createDefaultContainer();
	        return accessToken;
	      }
	    });
	  }
  
  
  protected Header[] getXStorageHeaders(PasswordCredentials cred){
	  return new Header[]{
			  new Header("X-Storage-User", cred.getUsername()), 
			  new Header("X-Storage-Pass", cred.getPassword())
			  };	  
  }

  /**
   * create default container if it doesn't exist for Hadoop Swift integration.
   * non-reentrant, as this should only be needed once.
   * @throws IOException IO problems.
   */
  private synchronized void createDefaultContainer() throws IOException {
    createContainer(container);
  }

  /**
   * Create a container -if it already exists, do nothing
   * @param containerName the container name
   * @throws IOException IO problems
   * @throws SwiftBadRequestException invalid container name
   * @throws SwiftInvalidResponseException error from the server
   */
  public void createContainer(String containerName) throws IOException {
    SwiftObjectPath objectPath = new SwiftObjectPath(containerName, "");
    try {
      //see if the data is there
      headRequest(objectPath, NEWEST);
    } catch (FileNotFoundException ex) {
      int status = 0;
      try {
        status = putRequest(objectPath);
      } catch (FileNotFoundException e) {
        //triggered by a very bad container name.
        //re-insert the 404 result into the status
        status = SC_NOT_FOUND;
      }
      if (status == SC_BAD_REQUEST) {
        throw new SwiftBadRequestException("Bad request " +
                                   "-possibly an illegal container name");
      }
      if (!isStatusCodeExpected(status,
                                SC_OK,
                                SC_CREATED,
                                SC_ACCEPTED,
                                SC_NO_CONTENT)) {
        throw new SwiftInvalidResponseException("Couldn't create container "
                                                + containerName +
                                                " for storing data in Swift." +
                                                " Try to create container " +
                                                containerName + " manually ",
                                                status,
                                                "PUT",
                                                null);
      }
      else {
        throw ex;
      }
    }
  }

  /**
   * Trigger an initial auth operation if some of the needed
   * fields are missing
   * @throws IOException on problems
   */
  private void authIfNeeded() throws IOException {
    if (getEndpointURI() == null) {
      authenticate();
    }
  }

  /**
   * Pre-execution actions to be performed by methods. Currently this
   * <ul>
   *   <li>Logs the operation at TRACE</li>
   *   <li>Authenticates the client -if needed</li>
   * </ul>
   * @throws IOException
   */
  private void preRemoteCommand(String operation) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Executing " + operation);
    }
    authIfNeeded();
  }



  /**
   * Performs the HTTP request, validates the response code and returns
   * the received data. HTTP Status codes are converted into exceptions.
   *
   * @param uri       URI to source
   * @param processor HttpMethodProcessor
   * @param <M>       method
   * @param <R>       result type
   * @return result of HTTP request
   * @throws IOException IO problems
   * @throws SwiftBadRequestException the status code indicated "Bad request"
   * @throws SwiftInvalidResponseException the status code is out of range
   * for the action (excluding 404 responses)
   * @throws SwiftInternalStateException the internal state of this client
   * is invalid
   * @throws FileNotFoundException a 404 response was returned
   */
  private <M extends HttpMethod, R> R perform(URI uri,
                      HttpMethodProcessor<M, R> processor)
      throws IOException, SwiftBadRequestException, SwiftInternalStateException,
             SwiftInvalidResponseException, FileNotFoundException {
    checkNotNull(uri);
    checkNotNull(processor);


    final M method = processor.createMethod(uri.toString());

    //retry policy
    HttpMethodParams methodParams = method.getParams();
    methodParams.setParameter(HttpMethodParams.RETRY_HANDLER,
                              new DefaultHttpMethodRetryHandler(
                                retryCount, false));
    methodParams.setSoTimeout(connectTimeout);

    try {
      int statusCode = exec(method);

      //look at the response and see if it was valid or not.
      //Valid is more than a simple 200; even 404 "not found" is considered
      //valid -which it is for many methods.

      //validate the allowed status code for this operation
      int[] allowedStatusCodes = processor.getAllowedStatusCodes();
      boolean validResponse = isStatusCodeExpected(statusCode,
                                                   allowedStatusCodes);

      if (!validResponse) {
        IOException ioe = buildException(uri, method, statusCode);
        throw ioe;
      }

      return processor.extractResult(method);
    } catch (IOException e) {
      //release the connection -always

      method.releaseConnection();
      throw e;
    }
  }

  /**
   * Build an exception from a failed operation. This can include generating
   * specific exceptions (e.g. FileNotFound), as well as the default
   * {@link SwiftInvalidResponseException}
   * {@link SwiftInvalidResponseException}.
   * @param uri URI for operation
   * @param method operation that failed
   * @param statusCode status code
   * @param <M> method type
   * @return an exception to throw.
   */
  private <M extends HttpMethod> IOException buildException(URI uri,
                                                            M method,
                                                            int statusCode) {
    IOException fault;

    //log the failure @debug level
    String errorMessage = String.format("Method %s on %s failed, status code: %d," +
                                        " status line: %s",
                                        method.getName(),
                                        uri,
                                        statusCode,
                                        method.getStatusLine()
                                       );
    if (LOG.isDebugEnabled()) {
      LOG.debug(errorMessage);
    }
    //send the command
    switch (statusCode) {
      case SC_NOT_FOUND:
        fault = new FileNotFoundException("Operation " + method.getName()
                                          + " on " + uri);
        break;
      case SC_BAD_REQUEST:
        //bad HTTP request
        fault =  new SwiftBadRequestException("Bad request against " + uri);
        break;

      case SC_REQUESTED_RANGE_NOT_SATISFIABLE:
        //out of range: end of the message
        fault = new EOFException(method.getStatusText());
        break;


      default:
        fault = new SwiftInvalidResponseException(
          errorMessage,
          statusCode,
          method.getName(),
          uri);
    }

    return fault;
  }

  /**
   * Exec a GET request and return the input stream of the response
   * @param uri URI to GET
   * @param requestHeaders request headers
   * @return the input stream. This must be closed to avoid log errors
   * @throws IOException
   */
  private InputStream doGet(final URI uri, final Header... requestHeaders)
       throws IOException {
    return perform(uri, new GetMethodProcessor<InputStream>() {
      @Override
      public InputStream extractResult(GetMethod method) throws IOException {
        return new HttpInputStreamWithRelease(uri, method);
      }

      @Override
      protected void setup(GetMethod method) throws
                                             SwiftInternalStateException {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Create an instance against a specific FS URI,
   *
   * @param filesystemURI filesystem to bond to
   * @param config source of configuration data
   * @return REST client instance
   * @throws IOException on instantiation problems
   */
  public static SwiftRestClient getInstance(URI filesystemURI,
                                            Configuration config) throws IOException {
    return new SwiftRestClient(filesystemURI, config);
  }


  /**
   * Convert the (JSON) data to a string request as UTF-8
   * @param data data
   * @return the data
   * @throws SwiftException if for some very unexpected reason it's impossible
   * to convert the data to UTF-8.
   */
  private static StringRequestEntity toJsonEntity(String data) throws
                                                               SwiftException {
    StringRequestEntity entity;
    try {
      entity = new StringRequestEntity(data, "application/json", "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new SwiftException("Could not encode data as UTF-8", e);
    }
    return entity;
  }

  /**
   * Converts Swift path to URI to make request.
   * This is public for unit testing
   *
   *
   * @param path            path to object
   * @param endpointURI damain url e.g. http://domain.com
   * @return valid URI for object
   * @throws SwiftException the path built from the endpoint and path not a URI
   */
  public static URI pathToURI(SwiftObjectPath path,
                              URI endpointURI) throws  SwiftException {
    checkNotNull(endpointURI, "Null Endpoint -client is not authenticated");

    String dataLocationURI = endpointURI.toString();
    try {

      dataLocationURI = SwiftUtils.joinPaths(dataLocationURI, encodeUrl(path.toUriPath()));
      return new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new SwiftException("Failed to create URI from " + dataLocationURI, e);
    }
  }

  /**
   * Encode the URL. This extends {@link URLEncoder#encode(String, String)}
   * with a replacement of + with %20.
   * @param url URL string
   * @return an encoded string
   * @throws SwiftException if the URL cannot be encoded
   */
  private static String encodeUrl(String url) throws SwiftException {
    if (url.matches(".*\\s+.*")) {
      try {
        url = URLEncoder.encode(url, "UTF-8");
        url = url.replace("+", "%20");
      } catch (UnsupportedEncodingException e) {
        throw new SwiftException("failed to encode URI", e);
      }
    }

    return url;
  }

  /**
   * Convert a swift path to a URI relative to the current endpoint.
   * @param path path
   * @return an path off the current endpoint URI.
   * @throws SwiftException
   */
  private URI pathToURI(SwiftObjectPath path) throws SwiftException {
    return pathToURI(path, getEndpointURI());
  }

  /**
   * Add the headers to the method, and the auth token (which must be set
   * @param method method to update
   * @param requestHeaders the list of headers
   * @throws SwiftInternalStateException not yet authenticated
   */
  private void setHeaders(HttpMethodBase method, Header[] requestHeaders)
      throws SwiftInternalStateException {
    for (Header header : requestHeaders) {
      method.addRequestHeader(header);
    }
    setAuthToken(method, getToken());
  }

  /**
   * Set the auth key header of the method to the token ID supplied
   * @param method method
   * @param accessToken access token
   * @throws SwiftInternalStateException if the client is not yet authenticated
   */
  private void setAuthToken(HttpMethodBase method, AccessToken accessToken)
      throws SwiftInternalStateException {
    checkNotNull(accessToken,"Not authenticated");
    method.addRequestHeader(HEADER_AUTH_KEY, accessToken.getId());
  }

  /**
   * Execute a method in a new HttpClient instance.
   * If the auth failed, authenticate then retry the method.
   * @param method methot to exec
   * @param <M> Method type
   * @return the status code
   * @throws IOException on any failure
   * @throws SwiftConnectionException failure to connect or authenticate
   */
  private <M extends HttpMethod> int exec(M method)
      throws IOException, SwiftConnectionException {
    final HttpClient client = new HttpClient();
    if (proxyHost != null) {
      client.getParams().setParameter(HTTP_ROUTE_DEFAULT_PROXY,
                                      new HttpHost(proxyHost, proxyPort));
    }

    int statusCode = execWithDebugOutput(method, client);
    if (method.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
      //unauthed -look at what raised the response

      if (method.getURI().toString().equals(authUri.toString())) {
        //unauth response from the AUTH URI itself.
        throw new SwiftConnectionException(
          "Authentication failed, URI credentials are incorrect,"
          + " or Openstack Keystone is configured incorrectly. URL='"
          + authUri + "' "
          + "username={" + username + "} "
          + "password length=" + password.length()
        );
      } else {
        //any other URL: try again
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reauthenticating");
        }
        authenticate();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Retrying original request");
        }
        statusCode = execWithDebugOutput(method, client);
      }
    }
    return statusCode;
  }

  /**
   * Execute the request with the request and response logged at debug level
   * @param method method to execute
   * @param client client to use
   * @param <M> method type
   * @return the status code
   * @throws IOException any failure reported by the HTTP client.
   */
  private <M extends HttpMethod> int execWithDebugOutput(M method,
                                                         HttpClient client)
                                                        throws IOException {
    if (LOG.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder(
        method.getName() + " " + method.getURI()+"\n");
      for (Header header:method.getRequestHeaders()) {
        builder.append(header.toString());
      }
      LOG.debug(builder);
    }
    int statusCode = client.executeMethod(method);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Status code = " + statusCode);
    }
    return statusCode;
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling
   * method is not null.
   *
   * @param reference an object reference
   * @return the non-null reference that was validated
   * @throws NullPointerException if {@code reference} is null
   */
  private static <T> T checkNotNull(T reference) throws
                                                 SwiftInternalStateException {
    return checkNotNull(reference, "Null Reference");
  }

  private static <T> T checkNotNull(T reference, String message) throws
                                                                 SwiftInternalStateException {
    if (reference == null) {
      throw new SwiftInternalStateException(message);
    }
    return reference;
  }

  /**
   * Check for a status code being expected -takes a list of expected values
   *
   * @param status received status
   * @param expected expected value
   * @return true iff status is an element of [expected]
   */
  private boolean isStatusCodeExpected(int status, int... expected) {
    for (int code : expected) {
      if (status == code) {
        return true;
      }
    }
    return false;
  }


  @Override
  public String toString() {
    return "SwiftRestClient: "+  filesystemURI ;
  }
}
