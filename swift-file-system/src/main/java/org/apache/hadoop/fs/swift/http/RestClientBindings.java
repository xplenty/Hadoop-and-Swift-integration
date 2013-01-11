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

package org.apache.hadoop.fs.swift.http;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;

import java.net.URI;
import java.util.Properties;

import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.*;

/**
 * This class implements the binding logic between Hadoop configurations
 * and the swift rest client.
 *
 * The swift rest client takes a Properties instance containing
 * the string values it uses to bind to a swift endpoint.
 *
 * This class extracts the values for a specific filesystem endpoint
 * and then builds an appropriate Properties file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class RestClientBindings {
  private static final Log LOG = LogFactory.getLog(RestClientBindings.class);


  public static String buildSwiftInstancePrefix(String hostname) {
    return SWIFT_SERVICE_PREFIX + hostname;
  }

  /**
   * Build a properties instance bound to the configuration file -using
   * the filesystem URI as the source of the information.
   * @param fsURI filesystem URI
   * @param conf configuration
   * @return a properties file with the instance-specific properties extracted
   * and bound to the swift client properties.
   * @throws SwiftConfigurationException if the configuration is invalid
   */
  public static Properties bind(URI fsURI, Configuration conf) throws
                                                               SwiftConfigurationException {
    String service = fsURI.getHost();
    if (service == null || service.isEmpty() || service.contains(".")) {
      //expect shortnames -> conf names
      throw new SwiftConfigurationException("Only short hostnames mapping to" +
                                            " a service binding are supported," +
                                            " not " + service
                                            + " (from) " + fsURI );
    }
    //build filename schema
    String prefix = buildSwiftInstancePrefix(service);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filesystem " + fsURI
                + " is using configuration keys " + prefix);
    }
    Properties props = new Properties();
    props.setProperty(SWIFT_SERVICE_PROPERTY, service);
    copy(conf, prefix + DOT_AUTH_URL, props, SWIFT_AUTH_PROPERTY, true);
    copy(conf, prefix + DOT_USERNAME, props, SWIFT_USERNAME_PROPERTY, true);
    copy(conf, prefix + DOT_PASSWORD, props, SWIFT_PASSWORD_PROPERTY, true);
    copy(conf, prefix + DOT_TENANT, props, SWIFT_TENANT_PROPERTY, false);
    copy(conf, prefix + DOT_REGION, props, SWIFT_REGION_PROPERTY, false);
    copy(conf, prefix + DOT_HTTP_PORT, props, SWIFT_HTTP_PORT_PROPERTY, false);
    copy(conf, prefix +
               DOT_HTTPS_PORT, props, SWIFT_HTTPS_PORT_PROPERTY, false);
    return props;

  }

  private static void set(Properties props, String key, String optVal) {
    if (optVal != null) {
      props.setProperty(key, optVal);
    }
  }

  /**
   * Copy a (trimmed) property from the configuration file to the properties file.
   *
   * If marked as required and not found in the configuration, an
   * exception is raised.
   * If not required -and missing- then the property will not be set.
   * In this case, if the property is already in the Properties instance,
   * it will remain untouched.
   * @param conf source configuration
   * @param confkey key in the configuration file
   * @param props destination property set
   * @param propsKey key in the property set
   * @param required is the property required
   * @throws SwiftConfigurationException if the property is required but was
   * not found in the configuration instance.
   */
  public static void copy(Configuration conf, String confkey, Properties props,
                          String propsKey,
                          boolean required) throws SwiftConfigurationException {
    String val = conf.getTrimmed(confkey);
    if (required && val == null) {
      throw new SwiftConfigurationException(
        "Missing mandatory configuration option: "
        +
        confkey);
    }
    set(props, propsKey, val);
  }


}
