# Integration tests for OpenStack&trade; Swift and Apache&trade; Hadoop&reg;


## Introduction

The swift-filesystem-integration project contains tests designed to test the
higher layers of the Apache&trade; Hadoop&reg stack against the
OpenStack&trade; Swift object Filesystem.

The test are designed to work with other filesystems so that
1. Developers can verify tests against local file:// and hdfs:// filesytems
before running tests against a remote Swift object store.
1. The same tests can be used to qualify other filesystems

Most of the tests are written in Groovy.

## Configuring the Tests

The tests are configured by way of the configuration XML file(s) in
`swiftfs-integration/src/test/resources/`

The `core-site.xml` XML file describes the basic attributes of the cluster,
including declaring the `swift:` filesystem implementation class. 

Although cluster binding information could be added there, this file is
under revision control -it is dangerously easy to accidentally commit
the security keys in to the SCM repository.

To prevent this, the base `core-site.xml` file imports a file `auth-keys.xml`
*which does not exist until created by the user*

This is another Hadoop-format XML file, which must be created.
All user-specific configuration options should go into this file.

### Creating the initial auth-keys.xml file

An initial `auth-keys.xml` file is:

    <configuration>
	</configuration>

### Adding cluster and security information

The cluster bindings are as defined in the Swift Filesystem XML files.

Here is an example binding to Rackspace US, using the "public" endpoint -which
means that the tests can run remotely against the service:

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
    <property>
      <name>fs.swift.service.rackspace.public</name>
      <value>true</value>
    </property>

*Warning*: using a public endpoint when running tests inside an OpenStack cluster
incurs the same charges as for remote access.


### Targeting a specific filesystem

Once a fileysystem binding has been defined, 
tests may be run against it.

	<property>
	  <name>test.fs.name</name>
	  <value>swift://container1.rackspace/</value>
	</property>  

Here the test uses the container `container1`; this container
must have been created in advance.

When the test are run, they will generate, read and write data under path  `/tmp/data`.

### Testing against the local filesystem

Test can be run against the local filesystem by setting that as the test filesystem.

	<property>
	  <name>test.fs.name</name>
	  <value>file:///</value>
	</property>  

All test cases work with files under `/tmp/data`. Accordingly, the directory `/tmp` must exist,
with write access by the current user. Its subdirectory `data` may exist -in which case
it must have same permissions. If not present, this subdirectory (and any others created during the tests)
will be created as needed.

Local filesytem testing is ideal for developing and debugging new test scripts, as access
is very fast. It can also be used to see if an unexpected behavior is
specific to the filesystem being tested.

### Tuning dataset sizes

The configuration option `test.fs.lines` can change the number of lines
of data generated, which in turn tunes the number of lines read
in by downstream tests.

    <property>
      <name>test.fs.lines</name>
      <value>3</value>
    </property>


### Customizing Logging

The log4J properties file in `swiftfs-integration/src/test/resources/log4j.properties`
can be tuned to increase or decrease the log output.
