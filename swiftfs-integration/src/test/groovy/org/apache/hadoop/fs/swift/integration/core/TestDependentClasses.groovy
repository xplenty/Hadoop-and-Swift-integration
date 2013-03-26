package org.apache.hadoop.fs.swift.integration.core

import org.junit.Test
import jline.ANSIBuffer
class TestDependentClasses {

  /**
   * This task verifies that JLine is on the classpath at compile time,
   * as Pig doesn't explicitly declare its dependency on it.
   * @throws Throwable
   */
  @Test
  public void testJLinePresent() throws Throwable {
    ANSIBuffer buffer = new ANSIBuffer();
  }
}
