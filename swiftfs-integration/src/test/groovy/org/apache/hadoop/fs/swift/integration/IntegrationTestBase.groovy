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

package org.apache.hadoop.fs.swift.integration

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.swift.integration.tools.DataGenerator
import org.apache.hadoop.fs.swift.util.Duration
import org.apache.hadoop.fs.swift.util.DurationStats
import org.apache.pig.ExecType
import org.apache.pig.PigServer
import org.apache.pig.data.Tuple
import org.apache.pig.impl.PigContext
import org.apache.pig.impl.util.PropertiesUtil
import org.junit.Assert
import org.junit.Before
import org.junit.internal.AssumptionViolatedException

@Commons
class IntegrationTestBase extends Assert implements Keys {


  private Configuration conf

  protected FileSystem bindFilesystem() {
    getSrcFilesystem();
  }

  @Before
  public void setup() {
    conf = createConfiguration()
  }

  def Configuration createConfiguration() {
    new Configuration()
  }

  protected FileSystem getSrcFilesystem() {
    URI serviceURI = getSrcFilesysURI(conf);
    return FileSystem.get(serviceURI, conf);
  }

  protected FileSystem getDestFilesystem() {
    URI serviceURI = getSrcFilesysURI(conf);
    return FileSystem.get(serviceURI, conf);
  }

  /**
   * This method exists to work around  HADOOP-9482.
   * It opens a file with a null permission rather than "the defaults",
   * so skips the checks for fs
   * @param fs
   * @param f
   * @param overwrite
   * @return
   */
  def FSDataOutputStream createFile(FileSystem fs, Path f, boolean overwrite) {
    def bufferSize = fs.getConf().getInt("io.file.buffer.size", 4096)
    def repl = fs.getDefaultReplication(f)
    def blocksize = fs.getDefaultBlockSize(f)
    fs.create(f, null, overwrite, bufferSize, repl, blocksize, null);
  }

  /**
   * Get the test URI
   * @param conf configuration
   * @throws IOException missing parameter or bad URI
   */
  public static URI getServiceURI(Configuration conf, String key) throws
      IOException {
    String instance = conf.get(key);
    if (instance == null) {
      throw new IOException(
          "Missing configuration entry " + key);
    }
    try {
      return new URI(instance);
    } catch (URISyntaxException e) {
      throw new IOException("Bad URI: " + instance);
    }
  }

  protected URI getSrcFilesysURI(Configuration conf) {
    getServiceURI(conf, Keys.KEY_TEST_FS);
  }

  protected URI getDestFilesysURI(Configuration conf) {
    getSrcFilesysURI(conf)
  }

  /**
   * Assume that a configuration option is set
   * @param conf configuration
   * @param key key to look for
   * @throws AssumptionViolatedException -this is converted to a test skip
   */
  protected void assumeSet(Configuration conf, String key) {
    skip(conf.get(key) == null, "Unset option " + key)
  }

  /**
   * skip a test if a condition is set 
   * @param condition condition to test
   * @param message message to use
   * @throws AssumptionViolatedException -this is converted to a test skip
   */
  protected void skip(boolean condition, String message) {
    if (condition) {
      throw new AssumptionViolatedException(message);
    }
  }

  /**
   * Create a pig server
   * @return
   */
  protected PigServer createPigServer() {
    Properties properties = PropertiesUtil.loadDefaultProperties()
    PigContext context = new PigContext(ExecType.LOCAL,
                                        properties)
    PigServer pig = new PigServer(context);
    pig
  }

  /**
   * Build the initial parameter map.
   * This includes injecting the source and dest 
   * parameters
   * @return a map
   */
  Map<String, String> paramMap(String srcpath) {
    def conf = createConfiguration()
    URI sourceURI = getSrcFilesysURI(conf);
    URI destURI = getDestFilesysURI(conf);
    def map = [:]
    map["src"] = sourceURI.toString();
    map["dest"] = destURI.toString();
    if (srcpath) map["srcfile"] = srcpath;
    map["destdir"] = DESTDIR;
    map
  }

  /**
   * Get the URI of the source by building a new URI from the 
   * source filesystem and the supplied path
   * @param srcpath path in the source filesystem
   * @return the full URI to the source
   */
  def URI sourceURI(String srcpath) {
    def conf = createConfiguration()
    URI sourceURI = getSrcFilesysURI(conf);
    URL sourceURL = sourceURI.toURL();
    new URL(sourceURL, srcpath);
  }

  def Path sourcePath(String srcDir) {
    def conf = createConfiguration()
    URI sourceURI = getSrcFilesysURI(conf);
    Path basePath = new Path(sourceURI)
    new Path(basePath,srcDir)
  }
  /**
   * Dump a map to the log at the info level
   * @param map map
   */
  protected void dumpMap(Map map) {
    map.each { k, v ->
      log.info("$k='$v'")
    }
  }

  /**
   * Convert a tuple into a tabbed { "field1" "field2" ... }
   * format
   * @param tuple
   * @return a strig for printing
   */
  String stringify(Tuple t) {
    StringBuilder sb = new StringBuilder("{")
    t.getAll().each { field ->
      sb.append(" \"")
      sb.append(field.toString())
      sb.append("\"")
    }
    sb.append(" }")
    sb.toString()
  }


  protected int dumpTuples(Iterator<Tuple> iterator, int limit) {
    int count = 0;
    iterator.each { ntuple ->
      String tstring = stringify(ntuple)
      if(count++<limit) {
        log.info(tstring)
      } else {
        log.debug(tstring)
      }
    }
    count
  }

  protected void registerPigResource(PigServer pig, String scriptResource, Map map) {
    InputStream stream = this.getClass().getClassLoader().
        getResourceAsStream(scriptResource)
    if (stream == null) {
      throw new FileNotFoundException("Could not load resource ${scriptResource}")
    }
    pig.registerScript(stream, map, null)
  }

  /**
   * Run the pig job anf get the "result" output
   * @param srcPath source path
   * @return the iterator over the results
   */
  def Iterator<Tuple> runBasePigJob(String srcPath) {
    String pigScript = "pig/loadgenerated.pig"
    PigServer pig = buildPigJob(srcPath, pigScript)
    def iterator = pig.openIterator("result")
    iterator
  }

  /**
   * Run a pig job
   * @param srcPath
   * @param pigScript
   * @return
   */
  def Iterator<Tuple> buildPigJob(String srcPath, String pigScript) {
    FileSystem fs = getSrcFilesystem();
    skip(!fs.exists(new Path(srcPath)),
         "No test data");
    PigServer pig = createPigServer()
    def map = paramMap(srcPath)
    dumpMap(map)
    //rm the dest dir
    FileSystem destFS = getDestFilesystem();
    destFS.delete(new Path(DESTDIR), true)
    registerPigResource(pig, pigScript, map)

  }

  def void generateManyFiles(DataGenerator generator, Path dataDir, int files) {
    FileSystem fs = getSrcFilesystem();
    log.info("Generating $files in $dataDir on $fs")
    fs.mkdirs(dataDir);
    deleteRobustly(fs, dataDir, 2);
    int failures;
    def stats = [:]
    stats["write"] = new DurationStats("write");
    stats["close"] = new DurationStats("close");
    stats["total"] = new DurationStats("total");
    for (fileindex in 1..files) {
      try {
        Path dataFile = new Path(dataDir, filename(fileindex))
        log.info("Writing $dataFile")
        Duration totalTime = new Duration()
        Duration createTime = new Duration()
        FSDataOutputStream out = createFile(fs, dataFile, true);
        createTime.finished()
        Duration writeTime = new Duration()
        generator.generate(out)
        writeTime.finished()
        Duration closeTime = new Duration();
        out.close();
        closeTime.finished()
        totalTime.finished()
        stats["write"].add(writeTime)
        stats["close"].add(closeTime)
        stats["total"].add(totalTime)
        log.info("Total time = $totalTime; create time=$createTime; write time =$writeTime; close time = $closeTime")
      } catch (IOException ioe) {
        log.warn("File $fileindex write failed $ioe", ioe)
        failures++;
      }
    }
    stats.each { log.info(it) }
    log.info("Failures: $failures; success rate = ${(files-failures*1.0)/files}")
    assert failures == 0
    //list that many files
    FileStatus[] statusList = fs.listStatus(dataDir)
    assert files == statusList.length
  }

  /**
   * Make a number of attempts to delete a directory; handles transient failures
   * better
   * @param fs filesystem
   * @param dataDir dir to delete
   * @param attempts how many attempts to make
   * @throws IOException after failing for the given max #of attempts
   */
  def void deleteRobustly(FileSystem fs, Path dataDir, int attempts)
              throws IOException {
    boolean success = false;
    int count=0;
    while (!success) {
      try {
        fs.delete(dataDir, true)
        success = true
      } catch (IOException e) {
        count++;
        log.warn("Attempt $count failed: $e")
        if (count > attempts) {
          throw e
        };
      }
    }
  }

  /**
   * Generate the filename for this generation test
   * @param fileindex index in the generation routine
   * @return a string for the filename
   */
  def String filename(int fileindex) {
    String.format("data-%05d", fileindex)
  }

}
