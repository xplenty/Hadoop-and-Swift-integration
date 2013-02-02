<!---
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
-->

# OpenStack&trade; Swift Filesystem Support for Apache&trade; Hadoop&reg;

This module enables Apache Hadoop applications -including MapReduce jobs,
read and write data to and from instances of the OpenStack Swift object store.

## Features

* Read and write of data stored in a Swift object store
* Support of a pseudo-hierachical file system (directories, subdirectories
and files)
* Standard filesystem operations: create, delete, mkdir, ls, mv, stat
* Can act as a source of data in a MapReduce job, or a sink.
* Support for multiple OpenStack services, and multiple containers from a single
service.
* Supports in-cluster and remote access to Swift data.
* Supports OpenStack Keystone authentication with password or token.
* Released under the Apache Software License
* Tested against the Hadoop 3.x and 1.x branches, against multiple public
OpenStack clusters: Rackspace US, Rackspace UK, HP Cloud.
* Tested against private openstack clusters, including scalability tests of
large file uploads.



## Using

### Concepts: services and containers

OpenStack swift is an *Object Store*; also known as a *blobstore*. It stores
arbitrary binary objects by name in a *container*.


The Swift filesystem client is designed to work with multiple swift filesystems,
both public and private. This allows the client to work with different
clusters, reading and writing data to and from either of them.


It can also work with the same cluster using multiple login details.

Both these features are achieved by one basic concept: using a service name
in the URI referring to a swift filesystem, and looking up all the connection
and login details for that specific service. Different service names
can be defined in the XML configuration file, so defining different clusters,
or providing different login details for the same cluster(s).

### Configuring

To talk to a swift service, one must provide
1. The URL defining the container and the service.
1. In the cluster/job configuration, the login details of that service.

Multiple service definitions can co-exist in the same configuration file
-just use different names for them.

#### Example: Rackspace US, in-cluster access using API key



    <property>
      <name>fs.swift.service.rackspace.auth.url</name>
      <value>https://auth.api.rackspacecloud.com/v2.0/tokens</value>
      <description>Rackspace US (multiregion)</description>
    </property>

    <property>
      <name>fs.swift.service.rackspace.username</name>
      <value>user4</value>
    </property>

    <property>
      <name>fs.swift.service.rackspace.region</name>
      <value>DFW</value>
    </property>

    <property>
      <name>fs.swift.service.rackspace.apikey</name>
      <value>fe806aa86dfffe2f6ed8</value>
    </property>

Here the API key visible in the account settings API keys page is used to log in.
No property for public/private access -the default is to use the private
endpoint for Swift operations.

This configuration also selects one of the regions, DFW, for its data.

A reference to this service would use the `rackspace` service name:

    swift://hadoop-container.rackspace/


#### Example: Rackspace UK service definition

    <property>
      <name>fs.swift.service.rackspaceuk.auth.url</name>
      <value>https://lon.identity.api.rackspacecloud.com/v2.0/tokens</value>
      <description>Rackspace UK</description>
    </property>

    <property>
      <name>fs.swift.service.rackspaceuk.username</name>
      <value>user4</value>
    </property>

    <property>
      <name>fs.swift.service.rackspaceuk.password</name>
      <value>insert-passphrase-here/value>
    </property>

    <property>
      <name>fs.swift.service.rackspace.public</name>
      <value>true</value>
    </property>

This is a public access point connection, using a password over an API key.

A reference to this service would use the `rackspaceuk` service name:

    swift://hadoop-container.rackspaceuk/



#### Example: HP cloud service definition


    <property>
      <name>fs.swift.service.hpcloud.auth.url</name>
      <value>https://region-a.geo-1.identity.hpcloudsvc.com:35357/v2.0/tokens
      </value>
      <description>HP Cloud</description>
    </property>

    <property>
      <name>fs.swift.service.hpcloud.username</name>
      <value>FE806AA86DFFFE2F6ED8</value>
    </property>

    <property>
      <name>fs.swift.service.hpcloud.password</name>
      <value>secret-password-goes-here</value>
    </property>

    <property>
      <name>fs.swift.service.hpcloud.public</name>
      <value>true</value>
    </property>

A reference to this service would use the `hpcloud` service name:

    swift://hadoop-container.hpcloud/


## Troubleshooting

### Failure to Authenticate

Check the auth URL through `curl` or your browser

### Timeout connecting to the Swift Service

This happens if the client application is running outside an OpenStack cluster,
where it does not have access to the private hostname/IP address for filesystem
operations. Set the public flag to true -but remember to set it to false
for use in-cluster.



## Warnings

1. Do not share your login details with anyone, which means do not log the
details, or check the XML configuration files into any revision control
system to which you do not have exclusive access.
1. Similarly, no use your real account details in any documentation.
1. Do not use the public service endpoint from within an OpenStack cluster,
as it will run up large bills.
1. Remember: it's not a real filesystem or hierarchical directory structure.
Some operations (directory rename and delete) take time and are not atomic or isolated
from other operations taking place.
1. Append is not supported.
1. Unix-style permissions are not supported. All accounts with write access
to a repository have unlimited access; the same goes for those with read access.
1. In the public clouds, do not make the containers public unless you are happy
with anyone reading your data, and are prepared to pay the costs of their
downloads.

## Limits

* Maximum length of an object path: 1024 characters
* Maximum size of a binary object: no absolute limit. Files > 5GB are partitioned
into separate files in the native filesystem, and merged during retrieval.

