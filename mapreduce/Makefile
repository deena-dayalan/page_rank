# Makefile for Hadoop MapReduce WordCount demo project.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/home/dasarathan/hadoop-3.2.2
jar.name=pagerank-mapreduce-1.0.jar
jar.path=target/${jar.name}
job.name=wc.PageRank
local.input=pagerank
local.interm-output=interm-output
local.output=output
local.log=log
build.dir=pagerank-mapreduce
tar.name=pagerank-mapreduce.tar.gz
local.k.param=3
local.iteration.param=5

# Pseudo-Cluster Execution
#------------------------------------------------------------
hdfs.user.name=dasarathan
hdfs.input=hdfs-pagerank
hdfs.output=hdfs-interm-output

# AWS EMR Execution
#------------------------------------------------------------
#aws.emr.release=emr-5.17.0
aws.emr.release=emr-6.2.0
aws.region=us-east-1
aws.bucket.name=pagerank-mapreduce
aws.k.param=10000
aws.iteration.param=10
#aws.subnet.id=subnet-6356553a
aws.input=pagerank
aws.output=output
aws.pr.output=pagerank_final
aws.log.dir=log
#aws.num.nodes=6
aws.master.nodes=1
aws.slave.nodes=5
aws.instance.type=m4.xlarge
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.interm-output}*
	rm -rf ${local.output}*
	rm -rf ${local.log}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.input} ${local.interm-output} ${local.k.param} ${local.iteration.param}
	echo "make local completed"

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.input}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output} ${local.k.param} ${local.iteration.param}
	make download-output-hdfs
	echo "make pseudo completed"

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output} ${local.k.param} ${local.iteration.param}
	make download-output-hdfs
	echo "make pseudoq completed"

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.log.dir}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
aws: jar make-bucket delete-bucket upload-input-aws upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "WordCount MR Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.slave.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":${aws.master.nodes},"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}","${aws.k.param}","${aws.iteration.param}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
	echo "make aws completed"

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.pr.output} ${local.output}
	mkdir ${local.log}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir} ${local.log}

# Get EMR cluster status
aws-status:
	aws emr list-clusters --active

# Delete S3 bucket.
delete-bucket:
	aws s3 rm s3://${aws.bucket.name} --recursive

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f ${tar.name}
	#rm -f ${zip.name}
	rm -rf ${build.dir}
	mkdir -p ${build.dir}
	cp -r src ${build.dir}
	cp -r config ${build.dir}
	cp -r input ${build.dir}
	cp -r output ${build.dir}
	cp -r target ${build.dir}
	cp -r log ${build.dir}
	cp pom.xml ${build.dir}
	cp Makefile ${build.dir}
	cp README.md ${build.dir}
	tar -czvf ${tar.name} ${build.dir}
	sleep 2
	rm -rf ${build.dir}
#	#cd build/deliv && zip -rq ../../MR-Demo.zip MR-Demo
