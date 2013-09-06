HADOOP-9912 test
======================
This tests symlink behaviors relevant to HADOOP-9912.

Building
-------------------------------------------------------------
In order to build and run this test, you must put the HDFS and Hadoop-common
jar files into your classpath.  In the Hadoop install, these are found under
share/hadoop/common/ share/hadoop/hdfs/

TODO: use Ivy.

Running
-------------------------------------------------------------
You have to set some Java system properties when running the test.

Here is an example of how to run the test:

    java com.cloudera.Hadoop9912Test hdfs://localhost:6000 

Contact information
-------------------------------------------------------------
Colin Patrick McCabe <cmccabe@alumni.cmu.edu>
