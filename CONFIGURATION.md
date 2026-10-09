# Spark to Clickzetta Job Configuration Guide

本文档介绍了 SparkToClickzettaJob 的所有配置参数，这些参数现在通过 `spark-submit` 的 `--conf` 选项传递。

## CMT Hive 导出入口

`com.clickzetta.connector.SourceExportJob` 是供 CMT Hive Spark 任务使用的 staging 导出入口。它通过
`SourceReaderFactory` 选择读取实现，目前 CMT 使用 `hive` reader，并固定以 Parquet overwrite 方式写入
任务级 `file://` 或 `cosn://` 目录。固定格式和写入模式是后续 CMT COPY 阶段的协议，不提供可覆盖参数。

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `spark.spark2cz.source.type` | String | 是 | 源类型；Hive 任务使用 `hive` |
| `spark.spark2cz.source.table` | String | 是 | Hive 表名，如 `` `sales`.`orders` `` |
| `spark.spark2cz.source.filter` | String | 否 | Spark SQL 过滤表达式 |
| `spark.spark2cz.export.path` | String | 是 | 任务级导出目录，支持 `file://` 或运行时已配置的对象存储 URI |

```bash
spark-submit \
  --class com.clickzetta.connector.SourceExportJob \
  --conf spark.spark2cz.source.type=hive \
  --conf 'spark.spark2cz.source.table=`sales`.`orders`' \
  --conf spark.spark2cz.export.path=file:///tmp/cmt/hive/job-1/task-1 \
  <spark2cz-shaded.jar>
```

COS/HDFS/Hive Metastore 的认证和连接参数属于 Spark/Hadoop 运行时配置，不属于上述应用参数。优先使用
集群角色或 Hadoop credential provider；使用静态凭证时应配置 Spark 的 redaction 规则，且不要把凭证放进
`spark.spark2cz.*` 参数。

## 使用方式

现在使用 `spark-submit` 命令运行作业，所有配置通过 `--conf` 参数传递：

```bash
# Local 模式
./run-local.sh

# 或者直接使用 spark-submit
spark-submit --class com.clickzetta.connector.SparkToClickzettaJob \
  --conf source.format=org.apache.doris.spark.sql.sources.DorisDataSource \
  --conf source.table.key=doris.table.identifier \
  ... \
  target/spark2cz-1.0-SNAPSHOT-jar-with-dependencies.jar

# Remote 集群模式
./run-remote.sh
```

## 配置参数说明

### 1. 数据源配置 (Source Configuration)

| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `source.format` | String | 是 | - | 源数据格式，如：`org.apache.doris.spark.sql.sources.DorisDataSource` |
| `source.table.key` | String | 是 | - | 源表标识键，如：`doris.table.identifier` |
| `source.table.values` | String | 是 | - | 要处理的表列表，逗号分隔，如：`db.table1,db.table2` |

### 2. 源连接器配置 (Source Connector Configuration)

这些配置会自动去掉 `source.connector.` 前缀后传递给源连接器。

#### Doris 连接器配置

| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `source.connector.doris.fenodes` | String | 是 | - | Doris FE 节点地址，如：`host:8410` |
| `source.connector.doris.user` | String | 是 | - | Doris 用户名 |
| `source.connector.doris.password` | String | 是 | - | Doris 密码 |
| `source.connector.doris.request.retries` | Integer | 否 | 3 | 请求重试次数 |
| `source.connector.doris.request.timeout` | Integer | 否 | 60000 | 请求超时时间（毫秒） |
| `source.connector.doris.request.batch.size` | Integer | 否 | 1024 | 批次大小 |
| `source.connector.doris.exec.mem.limit` | Long | 否 | 4294967296 | 执行内存限制（字节） |
| `source.connector.doris.request.tablet.size` | Integer | 否 | 10 | Tablet 大小 |
| `source.connector.doris.read.bitmap-to-string` | Boolean | 否 | true | 是否将 bitmap 类型读取为字符串 |

#### StarRocks 3.5 连接器配置

StarRocks 使用 `com.starrocks:starrocks-spark-connector-3.5_2.12:1.1.4`，与 Doris
连接器共存于同一个 shaded JAR。配置会去掉 `source.connector.` 前缀后原样传递给
StarRocks connector；因此这里的 key 使用 connector 文档中的 `starrocks.*` 名称。

| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `source.connector.starrocks.fe.http.url` | String | 是 | - | FE HTTP 地址，如 `http://host:8030` |
| `source.connector.starrocks.fe.jdbc.url` | String | 是 | - | FE MySQL/JDBC 地址，如 `jdbc:mysql://host:9030` |
| `source.connector.starrocks.user` | String | 是 | - | StarRocks 用户名 |
| `source.connector.starrocks.password` | String | 是 | - | StarRocks 密码 |
| `source.connector.starrocks.table.identifier` | String | 是 | - | 源表标识，如 `db.table` |
| `source.connector.starrocks.column.types` | String | 否 | - | 使用 Spark DDL 覆盖列类型；复杂类型需显式指定 |
| `source.connector.starrocks.bitmap.columns` | String | 否 | - | CMT 内部列清单；存在 Bitmap 列时自动切换公开 RDD 读取路径 |
| `source.connector.starrocks.filter.query` | String | 否 | - | StarRocks connector 过滤条件 |
| `source.connector.starrocks.request.retries` | Integer | 否 | 3 | HTTP 请求重试次数 |
| `source.connector.starrocks.request.connect.timeout.ms` | Integer | 否 | 30000 | HTTP 连接超时（毫秒） |
| `source.connector.starrocks.request.read.timeout.ms` | Integer | 否 | 30000 | HTTP 读取超时（毫秒） |
| `source.connector.starrocks.request.query.timeout.s` | Integer | 否 | 3600 | 查询超时（秒）；`-1` 表示不限制 |
| `source.connector.starrocks.request.tablet.size` | Integer | 否 | Integer.MAX_VALUE | 每个 Spark 分区包含的 tablet 数 |
| `source.connector.starrocks.batch.size` | Integer | 否 | 4096 | 每次从 BE 读取的行数；StarRocks 2.2+ 最小按 4096 处理 |
| `source.connector.starrocks.exec.mem.limit` | Long | 否 | 2147483648 | 单查询内存上限（字节） |
| `source.connector.starrocks.deserialize.arrow.async` | Boolean | 否 | false | 是否异步转换 Arrow 数据 |
| `source.connector.starrocks.deserialize.queue.size` | Integer | 否 | 64 | 异步 Arrow 转换队列大小 |
| `source.connector.starrocks.timezone` | String | 否 | Asia/Shanghai | DATETIME 转 Spark Timestamp 使用的时区 |

`starrocks` 是 Spark provider 短名；也可以在 `source.format` 中填写
`com.starrocks.connector.spark.sql.StarRocksDataSourceProvider`。当前 profile 固定
支持 StarRocks 3.5 connector 1.1.4。该版本没有 Doris 的
`read.bitmap-to-string` 开关。当前实现检测到 `BITMAP` 列后，使用 connector 公开 RDD API
在 FE 查询中执行 `bitmap_to_string`，再复用 Spark Binary/ClickZetta Bitmap 写入链路；
`HLL` 仍保持失败。

### 3. Clickzetta SDK 配置

这些配置会自动去掉 `clickzetta.sdk.` 前缀后传递给 Clickzetta SDK。

| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `clickzetta.sdk.url` | String | 是 | - | Clickzetta 数据库连接 URL |
| `clickzetta.sdk.schema` | String | 是 | - | 目标 schema 名称 |
| `clickzetta.sdk.format` | String | 否 | `com.clickzetta.spark.clickzetta.ClickzettaRelationProvider` | Clickzetta 格式提供者 |
| `clickzetta.sdk.access_mode` | String | 否 | `studio` | 访问模式 |
| `clickzetta.sdk.enable.results.verify` | Boolean | 否 | false | 是否启用结果验证 |
| `clickzetta.sdk.enable.concurrent.copy` | Boolean | 否 | false | 是否启用并发处理 |
| `clickzetta.sdk.save.mode` | String | 否 | `overwrite` | 保存模式：`append`、`overwrite`、`errorifexists`、`ignore` |
| `clickzetta.sdk.enable.bitmap_to_binary` | Boolean | 否 | false | 是否启用 bitmap 到 binary 的转换 |

#### Clickzetta Bulkload 配置

| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `clickzetta.sdk.cz.bulkload.prefer.internal.endpoint` | Boolean | 否 | false | 是否偏好内部端点 |
| `clickzetta.sdk.cz.bulkload.load.uri` | String | 否 | `file:///data1/bulkload-temp` | Bulkload 临时目录 |
| `clickzetta.sdk.cz.bulkload.load.format` | String | 否 | `parquet` | 数据格式，可选：`parquet`、`json` |
| `clickzetta.sdk.cz.bulkload.file.purge.enable` | Boolean | 否 | false | 是否启用文件清理 |
| `clickzetta.sdk.cz.bulkload.merge.into.table.volume.cleanup.enable` | Boolean | 否 | false | 是否启用表卷清理 |
| `clickzetta.sdk.cz.bulkload.local.file.cleanup.enable` | Boolean | 否 | false | 是否启用本地文件清理 |
| `clickzetta.sdk.writeVersion` | String | 否 | `V2` | 写入实现版本；如需规避 V2 路径可显式设为 `V1` |
| `clickzetta.sdk.writeVersionV2SplitUploadEnabled` | Boolean | 否 | false | 是否开启 V2 split upload，建议仅在明确验证后手动开启 |
| `cz.bulkload.sql.table.sink.supports.eq` | Boolean | 否 | true | SQL 表 sink 是否支持相等操作 |
| `clickzetta.sdk.spark.cz.bulkload.sql.cz.sql.table.sink.supports.eq` | Boolean | 否 | false | Spark Clickzetta SQL 表 sink 相等操作支持 |

### 4. Spark 层配置

| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `spark.filter.query` | String | 否 | - | Spark SQL 过滤条件，如：`dt between '2022-01-01' and '2022-12-31'` |
| `spark.target.partition.filter` | String | 否 | - | CMT 内部使用；存在时源数据已按该条件过滤，并使用 Lakehouse 动态 `INSERT OVERWRITE` 仅覆盖输入命中的分区；空结果才执行范围删除 |

### 5. Spark 运行时配置

这些是标准的 Spark 配置参数：

#### 基础配置

| 参数名 | 类型 | 建议值 | 说明 |
|--------|------|--------|------|
| `spark.sql.shuffle.partitions` | Integer | 500 | Shuffle 分区数 |
| `spark.default.parallelism` | Integer | 500 | 默认并行度 |
| `spark.rdd.compress` | Boolean | true | 是否压缩 RDD |

#### 内存配置

| 参数名 | 类型 | 建议值 | 说明 |
|--------|------|--------|------|
| `spark.executor.memoryOverhead` | String | 2g | Executor 内存开销 |
| `spark.local.dir` | String | /data1/spark-temp | Spark 临时目录 |
| `spark.sql.warehouse.dir` | String | /data1/spark-warehouse | SQL warehouse 目录 |

#### 日志配置

| 参数名 | 类型 | 建议值 | 说明 |
|--------|------|--------|------|
| `spark.eventLog.enabled` | Boolean | true | 是否启用事件日志 |
| `spark.eventLog.dir` | String | file:///data1/bi_doris/spark_event_logs | 事件日志目录 |

#### JVM 配置

| 参数名 | 类型 | 建议值 | 说明 |
|--------|------|--------|------|
| `spark.driver.extraJavaOptions` | String | `-Djava.io.tmpdir=/data1/java-temp` | Driver JVM 选项 |
| `spark.executor.extraJavaOptions` | String | `-Djava.io.tmpdir=/data1/java-temp` | Executor JVM 选项 |

## 配置示例

### Local 模式完整示例

```bash
spark-submit \
    --master local[*] \
    --class com.clickzetta.connector.SparkToClickzettaJob \
    --driver-memory 8g \
    --conf spark.sql.shuffle.partitions=500 \
    --conf source.format=org.apache.doris.spark.sql.sources.DorisDataSource \
    --conf source.table.key=doris.table.identifier \
    --conf source.table.values=db.table1,db.table2 \
    --conf source.connector.doris.fenodes=host:8410 \
    --conf source.connector.doris.user=username \
    --conf source.connector.doris.password=password \
    --conf clickzetta.sdk.url="jdbc:clickzetta://host:port/db" \
    --conf clickzetta.sdk.schema=schema_name \
    --conf clickzetta.sdk.save.mode=overwrite \
    target/spark2cz-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Cluster 模式完整示例

```bash
spark-submit \
    --master yarn 
    --deploy-mode cluster \
    --class com.clickzetta.connector.SparkToClickzettaJob \
    --driver-memory 8g \
    --executor-memory 16g \
    --num-executors 50 \
    --conf spark.sql.shuffle.partitions=500 \
    --conf source.format=org.apache.doris.spark.sql.sources.DorisDataSource \
    --conf source.table.key=doris.table.identifier \
    --conf source.table.values=db.table1,db.table2 \
    --conf source.connector.doris.fenodes=host:8410 \
    --conf source.connector.doris.user=username \
    --conf source.connector.doris.password=password \
    --conf clickzetta.sdk.url="jdbc:clickzetta://host:port/db" \
    --conf clickzetta.sdk.schema=schema_name \
    --conf clickzetta.sdk.save.mode=overwrite \
    target/spark2cz-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 迁移说明

### 原配置文件格式 vs 新配置格式

原来的配置文件 `config.dev.properties` 中的配置需要按以下规则转换：

1. **去掉前缀**：
   - `clickzetta.connector.spark.*` → `spark.*`
   - `source.connector.*` → 保持为 `source.connector.*`
   - `clickzetta.sdk.*` → 保持为 `clickzetta.sdk.*`

2. **Spark 配置处理**：
   - 以 `spark.` 开头的配置由 Spark 自动处理，无需在代码中手动设置
   - 原来的 `clickzetta.connector.spark.*` 配置需要去掉前缀变为 `spark.*`

### 示例转换

**原配置文件中的：**
```properties
clickzetta.connector.spark.sql.shuffle.partitions=500
clickzetta.connector.spark.executor.memoryOverhead=2g
source.connector.doris.fenodes=host:8410
clickzetta.sdk.schema=schema_name
```

**转换为 spark-submit 参数：**
```bash
--conf spark.sql.shuffle.partitions=500 \
--conf spark.executor.memoryOverhead=2g \
--conf source.connector.doris.fenodes=host:8410 \
--conf clickzetta.sdk.schema=schema_name
```

## 注意事项

1. **密码安全**：在生产环境中，建议使用环境变量或密钥管理系统来处理敏感信息，而不是直接在命令行中暴露密码
2. **路径配置**：确保所有路径（如临时目录、日志目录）在运行环境中存在且有相应权限
3. **资源配置**：根据集群资源和数据量调整内存、CPU 和执行器数量配置
4. **网络配置**：确保 Spark 集群能够访问 Doris 和 Clickzetta 服务
