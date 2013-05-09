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
package org.apache.hadoop.fs.swift.integration.mapred

import groovy.util.logging.Commons
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.hadoop.fs.swift.integration.tools.DotProgress
import org.apache.hadoop.fs.swift.util.SwiftTestUtils
import org.apache.hadoop.grumpy.GrumpyJob
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.*
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.TaskID
import org.apache.hadoop.mapreduce.TaskType
import org.junit.Test

@Commons
class TestRecordIO extends IntegrationTestBase {

  protected static final String ATTEMPT = "attempt";
  public static final String OUTDIR = "mapreduce.output.fileoutputformat.outputdir"

  @Test
  public void testCreateLineFile() throws Throwable {
    TextOutputFormat<Text, NullWritable> textOutputFormat =
      new TextOutputFormat<Text, NullWritable>();

    JobConf jobconf = createConfiguration()
    def srcFS = getSrcFilesystem();
    Path outDir = getSrcPath(RECORD_IO_DIR)
    deleteRobustly(srcFS, outDir, 2)
    String outdirs = outDir.toUri().toString()
    jobconf.set(JobContext.TASK_OUTPUT_DIR, outdirs)
    jobconf.set(OUTDIR, outdirs)
    String attempt = "attempt_001_002_m_004_005"
    //TaskAttemptID tid = TaskAttemptID.forName(attempt)
    TaskAttemptID tid = forName(attempt)
    jobconf.set(JobContext.TASK_ATTEMPT_ID, attempt);

    String lineio = "lineio"
    def recWrite = textOutputFormat.getRecordWriter(null,
                                                    jobconf,
                                                    lineio,
                                                    new DotProgress());
    def lines = jobconf.getInt(KEY_TEST_LINES, DEFAULT_TEST_RECORD_LINES)
    log.info("Generating ${lines} lines of records")
    Text text = new Text()
    1.upto(lines) { line ->
      text.set("Line ${line}")
      recWrite.write(text, NullWritable.get());
    }
    recWrite.close(Reporter.NULL);
    log.info("Generation complete: ")
    FileStatus[] stats = srcFS.listStatus(outDir);
    boolean found = false;
    stats.eachWithIndex { FileStatus entry, int i ->
      log.info("[$i]: + $entry");
      if (entry.path.toString().endsWith("lineio")) {
        assert entry.isFile();
        found = true;
      }
    }
    if (!found) {
      fail("Did not find $lineio file in "
               + SwiftTestUtils.dumpStats(outDir.toString(), stats))
    }

    //here the file exists, let's read it

  }


  @Test
  public void testLoadFif() throws Throwable {

    TextInputFormat fif = new TextInputFormat();
    JobConf jobconf = new JobConf();
    fif.configure(jobconf)
    GrumpyJob job = new GrumpyJob(jobconf, "noop")
    FileSystem fs = getSrcFilesystem()
    Path src = getSrcPath(RECORD_IO_DIR)
    skip(!fs.exists(src), "No test data")
    FileInputFormat.addInputPath(jobconf, src)
    List<FileStatus> statuses = fif.listStatus(jobconf)
    //run though
    def expectedFiles = expectedFileCount(jobconf)
    def (expectedMin, expectedMax) = expectedBlocksizeRange(jobconf)
    int listedFileCount = statuses.size()
    assert expectedFiles < 0 || listedFileCount == expectedFiles
    def lines = jobconf.getInt(KEY_TEST_LINES, DEFAULT_TEST_RECORD_LINES)
    statuses.eachWithIndex { FileStatus file, int index ->
      log.info("At [$index]: $file")
      long blockSize = file.getBlockSize();

      assert blockSize > expectedMin
      Path path = file.getPath();
      long length = file.getLen();
      assert length > 0
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      assert blkLocations.size() > 0
      //now actually read that data
      int total = 0;
      List<InputSplit> splits = fif.getSplits(jobconf, 8)
      splits.eachWithIndex { InputSplit split, int splitIndex ->

        RecordReader<LongWritable, Text> reader = fif.getRecordReader(split, jobconf, Reporter.NULL)

        def linesRead = readLines(reader)
        log.info("Read $linesRead lines")
        total += linesRead;

      }
      assert lines == total
    }

  }

  int readLines(RecordReader<LongWritable, Text> reader) {
    LongWritable offset = new LongWritable(0);
    Text text = new Text()
    int count = 0;
    while (reader.next(offset, text)) {
      log.info("[${offset}] $text")
      count++;
    }
    return count;
  }

  def expectedBlocksizeRange(JobConf conf) {
    [1024, 1024]
  }

  /**
   * expected  no. of files
   * @return the no of files or -1 for "don't know"
   */
  def expectedFileCount(JobConf conf) {
    1
  }

/**
 * only here to debug a problem with generating a manual attempt string
 * @param str
 * @return
 * @throws IllegalArgumentException
 */
  def TaskAttemptID forName(String str
  ) throws IllegalArgumentException {
    if (str == null) {
      return null
    };
    String exceptionMsg = null;
    try {
      String[] parts = str.split('_');
      parts.each { log.info(it) }
      if (parts.length == 6) {
        if (parts[0].equals(ATTEMPT)) {
          String type = parts[3];
          TaskType t = TaskID.getTaskType(type.charAt(0));
          if (t != null) {
            return new TaskAttemptID
            (parts[1],
             Integer.parseInt(parts[2]),
             t, Integer.parseInt(parts[4]),
             Integer.parseInt(parts[5]));
          } else {
            exceptionMsg = "Bad TaskType identifier. TaskAttemptId string : "
          }
          +str + " is not properly formed.";
        }
      }
    } catch (Exception ex) {
      log.error(ex)
      //fall below
    }
    if (exceptionMsg == null) {
      exceptionMsg = "TaskAttemptId string : " + str
      +" is not properly formed";
    }
    throw new IllegalArgumentException(exceptionMsg);
  }


}


