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

### Configuring


## Warnings

1. Do not share your login details with anyone, which means do not log the
details, or check the XML configuration files into any revision control
system to which you do not have exclusive access.
1. Do not use the public service endpoint from within an OpenStack cluster,
as it will run up large bills.
1. Remember: it's not a real filesystem or hierarchical directory structure.
Some operations (rename, delete) may take time and are not atomic or isolated
from other operations taking place.
1. Append is not supported.
1. Unix-style permissions are not supported. All accounts with write access
to a repository have unlimited access; the same goes for those with read access.


## Limits

* Maximum length of a file path: 1024 characters
