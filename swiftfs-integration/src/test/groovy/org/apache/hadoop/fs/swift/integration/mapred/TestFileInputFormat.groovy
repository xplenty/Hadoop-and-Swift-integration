package org.apache.hadoop.fs.swift.integration.mapred

import groovy.util.logging.Commons
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.hadoop.fs.swift.util.SwiftTestUtils
import org.apache.hadoop.grumpy.GrumpyJob
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.FileSystem

import org.junit.Test

@Commons

class TestFileInputFormat extends IntegrationTestBase {

  @Test
  public void testLoadFif() throws Throwable {
    DebugTextApi2InputFormat fif = new DebugTextApi2InputFormat();
    JobConf conf = new JobConf();
    GrumpyJob job = new GrumpyJob(conf, "noop")
    FileSystem fs = getSrcFilesystem()
    Path src = sourcePath(sourceDirectory(conf))
    skip(!fs.exists(src), "No test data")
    FileInputFormat.addInputPath(job, src)
    long maxSize = DebugTextApi2InputFormat.getMaxSplitSize(job);
    long minSplitSize = fif.getMinSplitSize(job)
    long formatMinSplitSize = fif.getFormatMinSplitSize()
    long minSize = Math.max(formatMinSplitSize, minSplitSize);
    String ls = SwiftTestUtils.ls(fs, src)
    List<FileStatus> statuses = fif.listStatus(job)
    //run though
    def expectedFiles = expectedFileCount(conf)
    def (expectedMin, expectedMax) = expectedBlocksizeRange(conf)
    int listedFileCount = statuses.size()
    assert expectedFiles < 0 || listedFileCount == expectedFiles
    
    statuses.eachWithIndex { FileStatus file, int index ->
      log.info("At [$index]: $file")
      long blockSize = file.getBlockSize();

      assert blockSize > expectedMin
      Path path = file.getPath();
      
      FileStatus stat = fs.getFileStatus(path)
      long length = file.getLen();
      assert stat.getLen() == file.getLen()
      assert file.getLen() > 0
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      assert blkLocations.size() > 0
      assert fif.isSplitable(job, file.path)
      long splitSize = fif.computeSplitSize(blockSize, minSize, maxSize);
      assert splitSize > minSize

      InputSplit[] splits = fif.getSplits(job, 4)
    }

  }

  @Test
  public void testJobConfMinSplit() throws Throwable {
    DebugTextApi2InputFormat fif = new DebugTextApi2InputFormat();
    JobConf conf = new JobConf();
    GrumpyJob job = new GrumpyJob(conf, "noop")

  }

  def String sourceDirectory(JobConf conf) {
    DATASET_CSV_PATH
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

}
