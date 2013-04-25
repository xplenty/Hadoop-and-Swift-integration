package org.apache.hadoop.fs.swift.integration.mapred

import groovy.util.logging.Commons
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

import org.apache.hadoop.fs.swift.integration.IntegrationTestBase
import org.apache.hadoop.grumpy.GrumpyJob
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.junit.Test

@Commons

class TestFileInputFormat extends IntegrationTestBase {

  @Test
  public void testLoadFif() throws Throwable {
    DebugTextInputFormat fif = new DebugTextInputFormat();
    JobConf conf = new JobConf();
    GrumpyJob job = new GrumpyJob(conf, "noop")
    FileSystem fs = getSrcFilesystem()
    Path src = sourcePath(sourceDirectory(conf))
    skip(!fs.exists(src), "No test data")
    FileInputFormat.addInputPath(job, src)
    long maxSize = DebugTextInputFormat.getMaxSplitSize(job);
    long minSplitSize = fif.getMinSplitSize(job)
    long formatMinSplitSize = fif.getFormatMinSplitSize()
    long minSize = Math.max(formatMinSplitSize, minSplitSize);
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
      long length = file.getLen();
      assert length>0
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      assert blkLocations.size() > 0
      assert fif.isSplitable(job,file.path)
      long splitSize = fif.computeSplitSize(blockSize, minSize, maxSize);
      assert splitSize > minSize
    }

  }

  @Test
  public void testJobConfMinSplit() throws Throwable {
    DebugTextInputFormat fif = new DebugTextInputFormat();
    JobConf conf = new JobConf();
    GrumpyJob job = new GrumpyJob(conf, "noop")

  }

  def String sourceDirectory(JobConf conf) {
    DATASET_CSV_PATH
  }
  
  def expectedBlocksizeRange(JobConf conf) {
    [1024,1024]
  }

  /**
   * expected  no. of files
   * @return the no of files or -1 for "don't know"
   */
  def expectedFileCount(JobConf conf) {
    1
  }
  
}
