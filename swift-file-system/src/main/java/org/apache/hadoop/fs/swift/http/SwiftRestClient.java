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

package org.apache.hadoop.fs.swift.http;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import static org.apache.commons.httpclient.HttpStatus.*;
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
import org.apache.hadoop.fs.swift.auth.AuthenticationRequest;
import org.apache.hadoop.fs.swift.auth.AuthenticationRequestWrapper;
import org.apache.hadoop.fs.swift.auth.AuthenticationResponse;
import org.apache.hadoop.fs.swift.auth.AuthenticationWrapper;
import org.apache.hadoop.fs.swift.auth.PasswordCredentials;
import org.apache.hadoop.fs.swift.auth.entities.AccessToken;
import org.apache.hadoop.fs.swift.auth.entities.Catalog;
import org.apache.hadoop.fs.swift.auth.entities.Endpoint;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swift.exceptions.SwiftConnectionException;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.exceptions.SwiftIllegalDataLocalityRequest;
import org.apache.hadoop.fs.swift.exceptions.SwiftInvalidResponseException;
import org.apache.hadoop.fs.swift.ssl.EasySSLProtocolSocketFactory;
import org.apache.hadoop.fs.swift.util.JSONUtil;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;

import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.*;

import org.jets3t.service.impl.rest.httpclient.HttpMethodReleaseInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

/**
 * This implements the client-side of the Swift REST API
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SwiftRestClient {
  private static final Log LOG = LogFactory.getLog(SwiftRestClient.class);
  private static final int RETRY_COUNT = 3;

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


  /**
   * objects query endpoint
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
  private synchronized void setAuthDetails(URI endpoint,
                                           URI objectLocation,
                                           AccessToken authToken) {
    endpointURI = endpoint;
    objectLocationURI = objectLocation;
    token = authToken;
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
        SC_PARTIAL_CONTENT,
        SC_CREATED,
        SC_NO_CONTENT,
        SC_ACCEPTED,
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
   * Create operation
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
   * @param conf The URI
   * @throws IOException
   */
  private SwiftRestClient(URI filesystemURI,
                          Configuration conf)
    throws IOException {


    Properties props = RestClientBindings.bind(filesystemURI, conf);
    String stringAuthUri = getOption(props, SWIFT_AUTH_PROPERTY);
    this.username = getOption(props, SWIFT_USERNAME_PROPERTY);
    this.password = getOption(props, SWIFT_PASSWORD_PROPERTY);
    //optional
    this.region = props.getProperty(SWIFT_REGION_PROPERTY);
    //tenant is optional
    this.tenant = props.getProperty(SWIFT_TENANT_PROPERTY);
    //service is used for diagnostics
    String service = props.getProperty(SWIFT_SERVICE_PROPERTY);

    if (LOG.isDebugEnabled()) {
      //everything you need for diagnostics. The password is omitted.
      LOG.debug(String.format(
        "Service={%s} uri={%s} tenant={%s} user={%s} region={%s}",
        service,
        stringAuthUri,
        tenant,
        username,
        region != null ? region : "(none)"
                             ));
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
   * @return byte[] file data
   */
  public InputStream getDataAsInputStream(SwiftObjectPath path, long offset, long length) throws IOException {
    if (offset < 0) {
      throw new IOException("Invalid offset: " + offset + ".");
    }
    if (length <= 0) {
      throw new IOException("Invalid length: " + length + ".");
    }

    final String range = String.format(SWIFT_RANGE_HEADER_FORMAT_PATTERN, offset, offset + length - 1);
    return getDataAsInputStream(path, new Header(HEADER_RANGE, range));
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
    return perform(uri, new HeadMethodProcessor<Long>() {
      @Override
      public Long extractResult(HeadMethod method) throws IOException {
        return method.getResponseContentLength();
      }
    });
  }

  public long getContentLength(SwiftObjectPath path) throws IOException {
    return getContentLength(pathToURI(path, endpointURI));
  }

  /**
   * Get the path contents as an input stream.
   *
   * @param path           path to file
   * @param requestHeaders http headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   */
  public InputStream getDataAsInputStream(SwiftObjectPath path,
                                          final Header... requestHeaders)
      throws IOException {

    return executeRequest(pathToURI(path, endpointURI),
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
    return perform(pathToURI(path, endpointURI),
                   new GetMethodProcessor<byte[]>() {
                     @Override
                     public byte[] extractResult(GetMethod method) throws
                                                                   IOException {
                       if (method.getStatusCode() == SC_NOT_FOUND) {
                         return null;
                       }
                       return method.getResponseBody();
                     }

                     @Override
                     protected void setup(GetMethod method) {
                       setHeaders(method, requestHeaders);
                     }
                   });
  }

  /**
   * Find objects under a prefix
   * @param path path prefix
   * @param requestHeaders optional request headers
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   */
  public byte[] findObjectsByPrefix(SwiftObjectPath path,
                                    final Header... requestHeaders) throws IOException {
    authIfNeeded();
    URI uri;
    String dataLocationURI = getEndpointURI().toString();
    try {
      String object = path.getObject();
      if (object.startsWith("/")) {
        object = object.substring(1);
      }

      dataLocationURI = dataLocationURI.concat("/").concat(path.getContainer().concat("/?prefix=").concat(object));
      uri = new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new SwiftException("Bad URI: " + dataLocationURI, e);
    }

    return perform(uri, new GetMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(GetMethod method) throws IOException {
        if (method.getStatusCode() == SC_NOT_FOUND) {
          //no result
          return null;
        }
        return method.getResponseBody();
      }

      @Override
      protected void setup(GetMethod method) {
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
   * @return true if the status code was consideres successful
   * @return byte[] file data or null if the object was not found
   * @throws IOException on IO Faults
   */
  public boolean copyObject(SwiftObjectPath src, final SwiftObjectPath dst, final Header... headers)
    throws IOException {
    return perform(pathToURI(src, endpointURI), new CopyMethodProcessor<Boolean>() {
      @Override
      public Boolean extractResult(CopyMethod method) throws IOException {
        return method.getStatusCode() != SC_NOT_FOUND;
      }

      @Override
      protected void setup(CopyMethod method) {
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

    perform(pathToURI(path, endpointURI), new PutMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(PutMethod method) throws IOException {
        return method.getResponseBody();
      }

      @Override
      protected void setup(PutMethod method) {
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

    return perform(pathToURI(path, endpointURI), new DeleteMethodProcessor<Boolean>() {
      @Override
      public Boolean extractResult(DeleteMethod method) throws IOException {
        return method.getStatusCode() == SC_NO_CONTENT;
      }

      @Override
      protected void setup(DeleteMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  public Header[] headRequest(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    return perform(pathToURI(path, endpointURI), new HeadMethodProcessor<Header[]>() {
      @Override
      public Header[] extractResult(HeadMethod method) throws IOException {
        if (method.getStatusCode() == SC_NOT_FOUND) {
          return null;
        }

        return method.getResponseHeaders();
      }

      @Override
      protected void setup(HeadMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  public int putRequest(SwiftObjectPath path, final Header... requestHeaders) throws IOException {
    return perform(pathToURI(path, endpointURI), new PutMethodProcessor<Integer>() {

      @Override
      public Integer extractResult(PutMethod method) throws IOException {
        return method.getStatusCode();
      }

      @Override
      protected void setup(PutMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Authenticate to Openstack Keystone
   * As well as returning the access token, the member fields {@link #token},
   * {@link #endpointURI} and {@link #objectLocationURI} are set up for re-use
   *
   * This method is re-entrant -if more than one thread attempts to authenticate
   * neither will block -but the field values with have those of the last caller
   * @return authenticated access token
   */
  public AccessToken authenticate() throws IOException {
    LOG.debug("started authentication");
    return perform(authUri, new PostMethodProcessor<AccessToken>() {
      @Override
      protected void setup(PostMethod method) throws SwiftException {
        AuthenticationRequest authRequest = new AuthenticationRequest(tenant, new
                                                               PasswordCredentials(
          username, password));
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
              URI endpointURL = endpoint.getPublicURL();
              descr = String.format("[%s =>  %s]; ", endpointRegion, endpointURL);
              regionList.append(descr);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Endpoint " + descr);
              }
              if (region == null || endpointRegion.equals(region)) {
                endpointURI = endpointURL;
                break;
              }
            }
          }
        }
        if (endpointURI == null) {
          throw new SwiftException("Could not find swift service from auth URL "
                                   + authUri
                                   + " and region '" + region + "'. "
                                   + "Categories: " + catList
                                   + ((regionList.length() > 0) ?
                                      ("regions: " + regionList)
                                      : "No regions")

          );
        }


        accessToken = access.getToken();
        String path = SWIFT_OBJECT_AUTH_ENDPOINT
                      + accessToken.getTenant().getId();
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
        createDefaultContainer(host);
        return accessToken;
      }
    });
  }

  /**
   * create default container if it doesn't exist for Hadoop Swift integration.
   * non-reentrant, as this should only be needed once.
   * @param host host object path
   * @throws IOException IO problems.
   */
  private synchronized void createDefaultContainer(String host) throws IOException {
    SwiftObjectPath objectPath = new SwiftObjectPath(host, "");
    try {
      getDataAsInputStream(objectPath);
    } catch (SwiftInvalidResponseException ex) {
      if (ex.statusCode == SC_NOT_FOUND) {
        final int status = putRequest(objectPath);
        if (!isStatusCodeExpected(status,
                                  SC_OK,
                                  SC_CREATED,
                                  SC_ACCEPTED,
                                  SC_NO_CONTENT)) {
          throw new SwiftInvalidResponseException("Couldn't create container "
                                                  + host +
                                                  " for storing data in Swift." +
                                                  " Try to create container " +
                                                  host + " manually ",
                                                  status,
                                                  "PUT",
                                                  null);
        } else {
          throw ex;
        }
      }
    }
  }

  /**
   * Trigger an initial auth operation if some of the needed
   * fields are missing
   * @throws IOException on problems
   */
  private void authIfNeeded() throws IOException {
    if (getEndpointURI()==null) {
      authenticate();
    }
  }

  /**
   * Performs request
   *
   * @param uri       URI to source
   * @param processor HttpMethodProcessor
   * @param <M>       method
   * @param <R>       result type
   * @return result of HTTP request
   */
  private <M extends HttpMethod, R> R perform(URI uri, HttpMethodProcessor<M, R> processor) throws IOException {
    checkNotNull(uri);
    checkNotNull(processor);


    final M method = processor.createMethod(uri.toString());
    
    //retry policy
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                                    new DefaultHttpMethodRetryHandler(
                                      RETRY_COUNT, false));

    try {
      int statusCode = exec(method);

      //look at the response and see if it was valid or not.
      //Valid is more than a simple 200; even 404 "not found" is considered
      //valid -which it is for many methods.

      if (statusCode == SC_BAD_REQUEST) {
        throw new SwiftIllegalDataLocalityRequest(
          "Bad request -probably an illegal path for a data locality test");
      }

      //validate the allowed status code for this operation
      int[] allowedStatusCodes = processor.getAllowedStatusCodes();
      boolean validResponse = isStatusCodeExpected(statusCode,
                                                   allowedStatusCodes);

      if (!validResponse)
        throw new SwiftInvalidResponseException(
          String.format("Method %s failed, status code: %d," +
                        " status line: %s (uri: %s)",
                method.getName(),
                statusCode,
                method.getStatusLine(),
                uri),
          statusCode,
          method.getName(),
          uri);

      return processor.extractResult(method);
    } catch (IOException e) {
      //release the connection -always
      method.releaseConnection();
      throw e;
    }
  }

  /**
   * Exec a GET request and return the input stream of the response
   * @param uri URI to GET
   * @param requestHeaders request headers
   * @return the
   * @throws IOException
   */
  private InputStream executeRequest(final URI uri, final Header... requestHeaders) throws IOException {
    return perform(uri, new GetMethodProcessor<InputStream>() {
      @Override
      public InputStream extractResult(GetMethod method) throws IOException {
        if (method.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          return null;
        }
        return new HttpMethodReleaseInputStream(method);
      }

      @Override
      protected void setup(GetMethod method) {
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
   */
  public static URI pathToURI(SwiftObjectPath path,
                              URI endpointURI) throws  SwiftException {
    String dataLocationURI = endpointURI.toString();
    try {
      if (path.toUriPath().startsWith("/")) {
        dataLocationURI = dataLocationURI.concat(path.toUriPath());
      } else {
        dataLocationURI = dataLocationURI.concat("/").concat(path.toUriPath());
      }

      return new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new SwiftException("Failed to create URI from " + dataLocationURI,
                               e);
    }
  }

  private void setHeaders(HttpMethodBase method, Header[] requestHeaders) {
    for (Header header : requestHeaders) {
      method.addRequestHeader(header);
    }
    setAuthToken(method, getToken());
  }

  /**
   * Set the auth key header of the method to the token ID supplied
   * @param method method
   * @param accessToken access token
   */
  private void setAuthToken(HttpMethodBase method, AccessToken accessToken) {
    method.addRequestHeader(HEADER_AUTH_KEY, accessToken.getId());
  }

  /**
   * Execute a method in a new HttpClient instance.
   * If the auth failed, authenticate then retry the method.
   * @param method methot to exec
   * @param <M> Method type
   * @return the status code
   * @throws IOException on any failure
   */
  private <M extends HttpMethod> int exec(M method) throws IOException {
    final HttpClient client = new HttpClient();
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

  private <M extends HttpMethod> int execWithDebugOutput(M method,
                                                         HttpClient client) throws
                                                                            IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing " + method.getName() + " " + method.getURI());
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
  private static <T> T checkNotNull(T reference) {
    if (reference == null) {
      throw new NullPointerException();
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
}
