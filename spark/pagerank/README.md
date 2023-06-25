# Spark(PAGE RANK)

Spring 2022

Code author
-----------
Deenadayalan Dasarathan

Installation
------------
These components are installed:
- JDK 1.8
- Scala 2.12.10
- Hadoop 3.2.2
- Spark 3.1.2 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example \~/.bash_aliases: <br />
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 <br />
export HADOOP_HOME=\~/hadoop-3.2.2 <br />
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop <br />
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop <br />
export SPARK_HOME=\~/spark-3.1.2-bin-without-hadoop <br />
export SCALA_HOME=~/scala-2.12.10 <br />
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin <br />
export HADOOP_CLASSPATH=$(/home/dasarathan/hadoop-3.2.2/bin/hadoop classpath) <br />
export SPARK_DIST_CLASSPATH=$(/home/dasarathan/hadoop-3.2.2/bin/hadoop classpath) <br />

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh: <br />
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64<br />

Execution
---------
All of the build & execution commands are organized in the Makefile.<br />
1) Unzip project file.<br />
2) Open command prompt.<br />
3) Navigate to directory where project files unzipped.<br />
4) Edit the Makefile to customize the environment at the top.<br />
	Sufficient for standalone: hadoop.root, jar.name, local.input<br />
	Other defaults acceptable for running standalone.<br />
5) Copy Twitter dataset (edges.csv and nodes.csv) to input directory.<br />
6) Standalone Hadoop:<br />
	make switch-standalone		-- set standalone Hadoop environment (execute once)<br />
	make local                  -- standalone execution<br />
7) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)<br />
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)<br />
	make pseudo					-- first execution<br />
	make pseudoq				-- later executions since namenode and datanode already running <br />
8) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)<br />
	make upload-input-aws		-- only before first execution<br />
	make aws					-- check for successful execution with web interface (aws.amazon.com)<br />
	make download-output-aws	-- after successful execution & termination<br />
	make aws-status				-- get the status of the active cluster<br />
	make delete-output-aws		-- remove output and log dir in the s3 bucket<br />
	make delete-bucket			-- delete all the contents of s3 bucket<br />
