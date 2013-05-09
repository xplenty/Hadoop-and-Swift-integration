package org.apache.hadoop.fs.swift.integration.tools

import org.apache.hadoop.util.Progress
import org.apache.hadoop.util.Progressable

/**
 * Print a dot to system.out
 */
class DotProgress implements Progressable {
  @Override
  void progress() {
    System.out.print('.')
  }
}
