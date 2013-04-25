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

interface Keys {
  /**
   * name of the key in the config XML files defining the filesystem to work with
   */
  String KEY_TEST_FS = "test.fs.name";
  /**
   * No of lines/records to generate
   */
  String KEY_TEST_LINES = "test.fs.lines";
  int DEFAULT_TEST_LINES = 8192;
  String KEY_TEST_FILES = "test.fs.files";
  int DEFAULT_TEST_FILES = 1;
  String KEY_TEST_MANY_FILES = "test.fs.many.files";
  int DEFAULT_TEST_MANY_FILES = 512;

  String KEY_TEST_FILESIZE_KB = "test.fs.filesize.kb";
  int DEFAULT_TEST_FILESIZE_KB = 10;

  String DATA_DIR = "/tmp/data/"
  String GENERATED_DATA_DIR = DATA_DIR +"generated/"
  String DATASET_CSV = "dataset.csv"
  String DATASET_CSV_PATH = DATA_DIR + "csv/dataset.csv"
  String DATASET_MASSIVE_BIN_PATH = DATA_DIR + "massive/bin"
  String DATASET_MANY_CSV_PATH = DATA_DIR + "many/csv"
  String DATASET_MASSIVE_CSV_PATH = DATA_DIR + "massive/csv"

  String DESTDIR = "/tmp/data/results"
  int DEFAULT_SEED = 500

  String KEY_SLEEP_INTERVAl = "test.fs.sleep.interval"
  int DEFAULT_SLEEP_INTERVAL = 1500

}
