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

* Support of a pseudo-hierachical file system (directories, subdirectories and
  files)

* Standard filesystem operations: `create`, `delete`, `mkdir`, `ls`, `mv`, `stat`

* Can act as a source of data in a MapReduce job, or a sink.

* Support for multiple OpenStack services, and multiple containers from a
  single service.

* Supports in-cluster and remote access to Swift data.

* Supports OpenStack Keystone authentication with password or token.

* Released under the Apache Software License

* Tested against the Hadoop 3.x and 1.x branches, against multiple public
  OpenStack clusters: Rackspace US, Rackspace UK, HP Cloud.

* Tested against private OpenStack clusters, including scalability tests of
  large file uploads.



## Using the Hadoop Swift Filesystem Client

## Concepts: services and containers

OpenStack swift is an *Object Store*; also known as a *blobstore*. It stores
arbitrary binary objects by name in a *container*. 

The Hadoop Swift filesystem library adds another concept, the *service*, which
defines which Swift blobstore hosts a container -and how to connect to it.

### Containers and Objects


Containers are created by users with accounts on the Swift filestore, and hold
*objects*.

* Objects can be zero bytes long, or they can contain data.

* Objects in the container can be up to 5GB; there is a special support for
  larger files than this, which merges multiple objects in to one.

* Each object is referenced by it's *name*. An object is named by its full
  name, such as `this-is-an-object-name`.

* You can use any characters in an object name that can be 'URL-encoded'; the
  maximum length of a name is 1034 characters -after URL encoding.

* Names can have `/` characters in them, which are used to create the illusion of
  a directory structure. For example `dir/dir2/name`. Even though this looks
  like a directory, *it is still just a name*. There is no requirement to have
  any entries in the container called `dir` or `dir/dir2`

* That said. if the container has zero-byte objects that look like directory
  names above other objects, they can pretend to be directories. Continuing the
  example, a 0-byte object called `dir` would tell clients that it is a
  directory while `dir/dir2` or `dir/dir2/name` were present. This creates an
  illusion of containers holding a filesystem.

Client applications talk to Swift over HTTP or HTTPS, reading, writing and
deleting objects using standard HTTP operations (GET, PUT and DELETE,
respectively). There is also a COPY operation, that creates a new object in the
container, with a new name, containing the old data. There is no rename
operation itself, objects need to be copied -then the original entry deleted.

The Swift Filesystem is *eventually consistent*: an operation on an object may
not be immediately visible to that client, or other clients. This is a
consequence of the goal of the filesystem: to span a set of machines, across
multiple datacenters, in such a way that the data can still be available when
many of them fail. (In contrast, the Hadoop HDFS filesystem is *immediately
consistent*, but it does not span datacenters.)

Eventual consistency can cause surprises for client applications that expect
immediate consistency: after an object is deleted or overwritten, the object
may still be visible -or the old data still retrievable. The Swift Filesystem
client for Apache Hadoop attempts to handle this, in conjunction with the
MapReduce engine, but there may be still be occasions when eventual consistency
causes surprises.

### Services

The Swift Filesystem client for Apache Hadoop is designed to work with multiple
Swift object stores, both public and private. This allows the client to work
with different clusters, reading and writing data to and from either of them.

It can also work with the same object stores using multiple login details.

Both these features are achieved by one basic concept: using a service name in
the URI referring to a swift filesystem, and looking up all the connection and
login details for that specific service. Different service names can be defined
in the Hadoop XML configuration file, so defining different clusters, or
providing different login details for the same object store(s).

# Working with Swift Object Stores in Hadoop


### Filesystem URIs

Hadoop uses URIs to refer to files within a filesystem. Some common examples
are:

    local://etc/hosts
    hdfs://cluster1/users/example/data/set1
    hdfs://cluster2.example.org:8020/users/example/data/set1

The Swift Filesystem Client adds a new URL type `swift`. In a Swift Filesystem
URL, the hostname part of a URL identifies the container and the service to
work with; the path the name of the object. Here are some examples

    swift://container.rackspace/my-object.csv
    swift://data.hpcloud/data/set1
    swift://dmitry.privatecloud/out/results

In the last two examples, the paths look like directories: it is not, they are
simply the objects named `data/set1` and `out/results` respectively.

## Installing

The `hadoop-swift` JAR must be on the classpath of the Hadoop program trying to
talk to the Swift service. If installed in the classpath of the Hadoop
MapReduce service, then all programs started by the MR engine will pick up the
JAR automatically. This is the easiest way to give all Hadoop jobs access to
Swift.

Alternatively, the JAR can be included as one of the JAR files that an
application uses. This lets the Hadoop jobs work with a Swift object store even
if the Hadoop cluster is not pre-configured for this.

Other applications may use the Swift Filesystem library, by way of the Hadoop
filesystem APIs. This allows programs which interact with a Hadoop cluster to
also read and write data in a Swift object store.

## Configuring

To talk to a swift service, you must provide:

1. The URL defining the container and the service.
1. In the cluster/job configuration, the login details of that service.

Multiple service definitions can co-exist in the same configuration file: just
use different names for them.

#### Example: Rackspace US, in-cluster access using API key

This service definition is for use in a Hadoop cluster deployed within Rackspace's
US infrastructure.

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

Here the API key visible in the account settings API keys page is used to log
in. No property for public/private access -the default is to use the private
endpoint for Swift operations.

This configuration also selects one of the regions, DFW, for its data.

A reference to this service would use the `rackspace` service name:

    swift://hadoop-container.rackspace/


#### Example: Rackspace UK: remote access with password authentication

This connects to Rackspace's UK ("LON") datacenter.

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
      <value>insert-password-here/value>
    </property>

    <property>
      <name>fs.swift.service.rackspace.public</name>
      <value>true</value>
    </property>

This is a public access point connection, using a password over an API key.

A reference to this service would use the `rackspaceuk` service name:

    swift://hadoop-container.rackspaceuk/

Because the public endpoint is used, if this service definition is used within
the London datacenter, all accesses will be billed at the public
upload/download rates, *irrespective of where the Hadoop cluster is*.

#### Example: HP cloud service definition

Here is an example that connects to the HP Cloud object store.

    <property>
      <name>fs.swift.service.hpcloud.auth.url</name>
      <value>https://region-a.geo-1.identity.hpcloudsvc.com:35357/v2.0/tokens
      </value>
      <description>HP Cloud</description>
    </property>

    <property>
      <name>fs.swift.service.hpcloud.tenant</name>
      <value>FE806AA86</value>
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

### General Swift Filesystem configuration options

Some configuration options apply to the Swift client, independent of
the specific Swift filesystem chosen.

#### Connection timeout `fs.swift.connect.timeout`

This sets the timeout in milliseconds to connect to a Swift service.

    <property>
      <name>fs.swift.connect.timeout</name>
      <value>15000</value>
    </property>

A shorter timeout means that connection failures are raised faster -but
may trigger more false alarms. A longer timeout is more resilient to network
problems -and may be needed when talking to remote filesystems.


#### Connection Retry Count `fs.swift.connect.retry.count`

This sets the number of times to try to connect to a service whenever
an HTTP request is made.

    <property>
      <name>fs.swift.connect.retry.count</name>
      <value>3</value>
    </property>

The more retries, the more resilient it is to transient outages -and the
less rapid it is at detecting and reporting server connectivity problems.

#### HTTP Proxy 

If the client can only access the Swift filesystem via a web proxy
server, the client configuration must specify the proxy via
the `fs.swift.connect.proxy.host` and `fs.swift.connect.proxy.port`
properties.

    <property>
      <name>fs.swift.proxy.host</name>
      <value>web-proxy</value>
    </property>

    <property>
      <name>fs.swift.proxy.port</name>
      <value>8088</value>
    </property>

If the host is declared, the proxy port must be set to a valid integer value.


## Troubleshooting

### ClassNotFoundException

The swift filesystem JAR may not be on your classpath.

If it is a remote MapReduce job that is failing, make sure that the JAR is
installed on the servers in the cluster -or that the job submission process
uploads the JAR file to the distributed cache.

### Failure to Authenticate

A `SwiftAuthenticationFailedException` is thrown when the client
cannot authenticate with the OpenStack keystone server. This could be
because the URL in the service definition is wrong, or because
the supplied credentials are invalid.

1. Check the authentication URL through `curl` or your browser

1. Use a Swift client such as CyberDuck to validate your credentials

1. If you have included a tenant ID, try leaving it out. Similarly,
try adding it if you had not included it.

1. Try switching from API key authentication to password-based authentication,
by setting the password.

### Timeout connecting to the Swift Service

This happens if the client application is running outside an OpenStack cluster,
where it does not have access to the private hostname/IP address for filesystem
operations. Set the public flag to true -but remember to set it to false
for use in-cluster.

## Warnings

1. Do not share your login details with anyone, which means do not log the
details, or check the XML configuration files into any revision control system
to which you do not have exclusive access.

1. Similarly, do not use your real account details in any documentation
* or any bug reports submitted online*

1. Prefer the apikey authentication over passwords as it is easier
to revoke a key -and some service providers allow you to set
an automatic expiry date on a key when issued.

1. Do not use the public service endpoint from within a public OpenStack
cluster, as it will run up large bills.

1. Remember: it's not a real filesystem or hierarchical directory structure.
Some operations (directory rename and delete) take time and are not atomic or
isolated from other operations taking place.

1. Append is not supported.

1. Unix-style permissions are not supported. All accounts with write access to
a repository have unlimited access; the same goes for those with read access.

1. In the public clouds, do not make the containers public unless you are happy
with anyone reading your data, and are prepared to pay the costs of their
downloads.

## Limits

* Maximum length of an object path: 1024 characters

* Maximum size of a binary object: no absolute limit. Files > 5GB are
  partitioned into separate files in the native filesystem, and merged during
  retrieval.

