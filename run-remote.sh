#!/bin/bash

# Spark to Clickzetta Job - Remote Cluster Execution Script
# This script runs the job on a remote Spark cluster (YARN) using spark-submit

# Set default values
JAR_FILE="target/spark2cz-1.0-SNAPSHOT-jar-with-dependencies.jar"
SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
MASTER=${MASTER:-"yarn"}
DEPLOY_MODE=${DEPLOY_MODE:-"cluster"}

# Check if jar file exists
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file $JAR_FILE not found!"
    echo "Please build the project first: mvn clean package"
    exit 1
fi

# Check if SPARK_HOME is set and spark-submit exists
if [ ! -f "$SPARK_HOME/bin/spark-submit" ]; then
    echo "Error: spark-submit not found at $SPARK_HOME/bin/spark-submit"
    echo "Please set SPARK_HOME environment variable or install Spark"
    exit 1
fi

echo "Starting Spark to Clickzetta Job in Remote Cluster Mode..."
echo "JAR File: $JAR_FILE"
echo "Spark Home: $SPARK_HOME"
echo "Master: $MASTER"
echo "Deploy Mode: $DEPLOY_MODE"
echo "=========================================="

# Execute spark-submit with cluster configuration
$SPARK_HOME/bin/spark-submit \
    --master $MASTER \
    --deploy-mode $DEPLOY_MODE \
    --class com.clickzetta.connector.SparkToClickzettaJob \
    --name "SparkToClickzettaJob" \
    --driver-memory 8g \
    --driver-cores 4 \
    --executor-memory 16g \
    --executor-cores 4 \
    --num-executors 50 \
    --executor-dynamic-allocation false \
    --driver-java-options "-Xmx8g -Xms8g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC" \
    --executor-java-options "-XX:+UseG1GC -Djava.io.tmpdir=/data1/java-temp" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://namenode:9000/spark-logs \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.default.parallelism=500 \
    --conf spark.rdd.compress=true \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.local.dir=/data1/spark-temp \
    --conf spark.sql.warehouse.dir=hdfs://namenode:9000/spark-warehouse \
    --conf spark.yarn.archive=hdfs://namenode:9000/spark-jars/spark-archive.zip \
    --conf spark.yarn.stagingDir=hdfs://namenode:9000/user/spark/staging \
    --conf spark.yarn.historyServer.address=historyserver:18080 \
    --conf spark.source.format=org.apache.doris.spark.sql.sources.DorisDataSource \
    --conf spark.source.table.key=doris.table.identifier \
    --conf spark.source.table.values=db.table1 \
    --conf spark.source.connector.doris.fenodes=compar*******i.com:8410 \
    --conf spark.source.connector.doris.user=peison*******_cluster \
    --conf spark.source.connector.doris.password=******* \
    --conf spark.source.connector.doris.request.retries=3 \
    --conf spark.source.connector.doris.request.timeout=60000 \
    --conf spark.source.connector.doris.request.batch.size=1024 \
    --conf spark.source.connector.doris.exec.mem.limit=4294967296 \
    --conf spark.source.connector.doris.request.tablet.size=10 \
    --conf spark.source.connector.doris.read.bitmap-to-string=true \
    --conf spark.filter.query="dt between '2022-01-01' and '2022-12-31'" \
    --conf spark.clickzetta.sdk.url="jdbc:clickzetta://9f8424ae.host:8033/quick_start?username=mt_admin&password=******%23&schema=peisong_smv&virtualCluster=default&use_http=true" \
    --conf spark.clickzetta.sdk.schema=peisong_smv \
    --conf spark.clickzetta.sdk.format=com.clickzetta.spark.clickzetta.ClickzettaRelationProvider \
    --conf spark.clickzetta.sdk.access_mode=studio \
    --conf spark.clickzetta.sdk.enable.results.verify=false \
    --conf spark.clickzetta.sdk.enable.concurrent.copy=false \
    --conf spark.clickzetta.sdk.save.mode=overwrite \
    --conf spark.clickzetta.sdk.enable.bitmap_to_binary=false \
    --conf spark.clickzetta.sdk.cz.bulkload.prefer.internal.endpoint=false \
    --conf spark.clickzetta.sdk.cz.bulkload.load.uri=hdfs://namenode:9000/bulkload-temp \
    --conf spark.clickzetta.sdk.cz.bulkload.file.purge.enable=false \
    --conf spark.clickzetta.sdk.cz.bulkload.merge.into.table.volume.cleanup.enable=false \
    --conf spark.clickzetta.sdk.cz.bulkload.local.file.cleanup.enable=false \
    --conf spark.cz.bulkload.sql.table.sink.supports.eq=true \
    --conf spark.clickzetta.sdk.spark.cz.bulkload.sql.cz.sql.table.sink.supports.eq=false \
    $JAR_FILE

echo "=========================================="
echo "Spark to Clickzetta Job submitted to cluster"
echo "You can monitor the job progress in Spark UI and YARN ResourceManager"