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

package com.cloudera;

import java.io.InputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.Thread;
import java.lang.System;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This tests symlink behaviors in Hadoop.
 */
public class Hadoop9912Test { //extends Configured {
  public static void main(String[] args) throws Exception {
    System.out.println("running Hadoop9912 test: tests symlink " +
        "behaviors in Hadoop...\n");

    if (args.length < 1) {
      System.err.println("You must specify a single argument: the URI " +
          "of a directory to test.\n" +
          "Examples: file:///tmp, hdfs:///\n");
      System.exit(1);
    }
    String uri = args[0];
    FileContext fc = FileContext.getFileContext(new URI(uri));
    Path testDir = new Path(new Path(uri), "testdir");
    System.out.println("deleting " + testDir + "...");
    if (fc.util().exists(testDir)) {
      if (!fc.delete(testDir, true)) {
        System.err.println("Failed to delete " + testDir);
        System.exit(1);
      }
    }
    System.out.println("mkdir " + testDir);
    try {
      fc.mkdir(testDir, new FsPermission((short)0755), true);
    } catch (IOException e) {
      System.err.println("Failed to mkdir " + testDir + "; " + e);
      System.exit(1);
    }
    Path dangling = new Path(testDir, "dangling"); 
    System.out.println("ln -s missing " + dangling);
    fc.createSymlink(new Path("missing"), dangling, false);
    System.out.println("mkdir beta");
    Path beta = new Path(testDir, "beta");
    fc.mkdir(beta, new FsPermission((short)0755), true);
    Path alpha = new Path(testDir, "alpha");
    System.out.println("ln -s beta " + alpha);
    fc.createSymlink(new Path("beta"), alpha, false); 
    RemoteIterator<FileStatus> statuses = fc.listStatus(testDir);
    System.out.println("listStatus " + testDir + ":");
    while (statuses.hasNext()) {
      FileStatus status = statuses.next();
      System.out.println(fileStatusToString(status));
    }
    System.out.println();

    Path globPath = new Path(testDir, "*");
    System.out.println("globStatus " + globPath + ":");
    FileStatus globStatuses[] = fc.util().globStatus(globPath);
    for (FileStatus status : globStatuses) {
      System.out.println(fileStatusToString(status));
    }
    System.out.println();

    globPath = new Path(testDir, "alpha");
    System.out.println("globStatus " + globPath + ":");
    globStatuses = fc.util().globStatus(globPath);
    for (FileStatus status : globStatuses) {
      System.out.println(fileStatusToString(status));
    }
    System.out.println();

    Path fileLinkStatusPath = new Path(testDir, "alpha");
    System.out.println("getFileLinkStatus " + fileLinkStatusPath + ":");
    System.out.println(fileStatusToString(
          fc.getFileLinkStatus(fileLinkStatusPath)));
  }

  private static String fileStatusToString(FileStatus s) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("path:" + s.getPath());
    sb.append("; isDirectory=" + s.isDir());
    sb.append("; isSymlink=" + s.isSymlink());
    if(s.isSymlink()) {
      sb.append("; symlink=" + s.getSymlink());
    }
    return sb.toString();
  }
}
