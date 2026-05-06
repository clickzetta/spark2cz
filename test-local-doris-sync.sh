#!/bin/bash

# Spark to Clickzetta Job - Local Execution Script
# This script runs the job in local mode using spark-submit

# Set default values
JAR_FILE="spark2cz-1.0-SNAPSHOT-shaded.jar"
SPARK_HOME=${SPARK_HOME:-"/opt/spark"}

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

echo "Starting Spark to Clickzetta Job in Local Mode..."
echo "JAR File: $JAR_FILE"
echo "Spark Home: $SPARK_HOME"
echo "=========================================="

mkdir -p /data/bi_doris/spark_event_logs /data/bulkload-temp /data/spark-warehouse /data/spark-temp

# Execute spark-submit with configuration from original config.dev.properties
$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --class com.clickzetta.connector.SparkToClickzettaJob \
    --driver-memory 8g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=file:///data/bi_doris/spark_event_logs \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.default.parallelism=500 \
    --conf spark.rdd.compress=true \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.local.dir=/data/spark-temp \
    --conf spark.sql.warehouse.dir=/data/spark-warehouse \
    --conf spark.source.format=org.apache.doris.spark.sql.sources.DorisDataSource \
    --conf spark.source.table.key=doris.table.identifier \
    --conf spark.source.table.values=testdb.pv_bitmap \
    --conf spark.source.connector.doris.fenodes=127.0.0.1:8030 \
    --conf spark.source.connector.doris.user=root \
    --conf spark.source.connector.doris.password=root \
    --conf spark.source.connector.doris.request.retries=3 \
    --conf spark.source.connector.doris.request.timeout=60000 \
    --conf spark.source.connector.doris.request.batch.size=1024 \
    --conf spark.source.connector.doris.exec.mem.limit=4294967296 \
    --conf spark.source.connector.doris.request.tablet.size=10 \
    --conf spark.source.connector.doris.read.bitmap-to-string=true \
    --conf spark.filter.query="dt > 0" \
    --conf spark.clickzetta.sdk.url="jdbc:clickzetta://instance_id.uat-api.clickzetta.com/open_source?schema=public&virtualcluster=default&username=***&password=***&use_http=true" \
    --conf spark.clickzetta.sdk.schema=public \
    --conf spark.clickzetta.sdk.format=com.clickzetta.spark.clickzetta.ClickzettaRelationProvider \
    --conf spark.clickzetta.sdk.access_mode=studio \
    --conf spark.clickzetta.sdk.enable.results.verify=false \
    --conf spark.clickzetta.sdk.enable.concurrent.copy=false \
    --conf spark.clickzetta.sdk.save.mode=overwrite \
    --conf spark.clickzetta.sdk.enable.bitmap_to_binary=true \
    --conf spark.clickzetta.sdk.cz.bulkload.prefer.internal.endpoint=false \
    --conf spark.clickzetta.sdk.cz.bulkload.load.uri=file:///data/bulkload-temp \
    --conf spark.clickzetta.sdk.cz.bulkload.file.purge.enable=false \
    --conf spark.clickzetta.sdk.cz.bulkload.merge.into.table.volume.cleanup.enable=false \
    --conf spark.clickzetta.sdk.cz.bulkload.local.file.cleanup.enable=false \
    --conf spark.cz.bulkload.sql.table.sink.supports.eq=true \
    --conf spark.clickzetta.sdk.spark.cz.bulkload.sql.cz.sql.table.sink.supports.eq=false \
    $JAR_FILE

echo "=========================================="
echo "Spark to Clickzetta Job completed"