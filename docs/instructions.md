# Instructions for use

This series of sample codes mainly shows how to use spark and spark doris connector to read data from doris table.
here give some code examples using java and scala language.

```java
com.clickzetta.connector.SparkToClickzettaJob
```


1. First, create a table in doris with any mysql client

   ```sql
   CREATE TABLE `example_table` (
   `id` bigint(20) NOT NULL COMMENT "ID",
   `name` varchar(100) NOT NULL COMMENT "Name",
   `age` int(11) NOT NULL COMMENT "Age"
   ) ENGINE = OLAP
   UNIQUE KEY(`id`)
   COMMENT "example table"
   DISTRIBUTED BY HASH(`id`) BUCKETS 1
   PROPERTIES (
   "replication_num" = "1",
   "in_memory" = "false",
   "storage_format" = "V2"
   );
   ```

2. Insert some test data to example_table

   ```sql
   insert into example_table values(1,"xx1",21);
   insert into example_table values(2,"xx2",21);
   ```

3. Set doris config file path and Run this jar
```shell
export clickzetta_config=/tmp/config.properties && java -Xmx8g -Xms8g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC -jar /tmp/spark2cz-1.0-SNAPSHOT-jar-with-dependencies.jar
```

change the Doris source.table.values, source.connector.* properties in the config file.
