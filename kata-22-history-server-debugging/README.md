# Kata 22: History Server & Debugging

## Goal
Master Spark History Server setup, application debugging, and production optimization techniques.

## Spark History Server

```
HISTORY SERVER OVERVIEW
═══════════════════════════════════════════════════════════════════════════════

  The History Server provides a web UI for viewing completed Spark applications.
  Essential for post-mortem analysis and debugging production jobs.

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        SPARK HISTORY SERVER                                 │
  │                                                                             │
  │    http://history-server:18080                                              │
  │                                                                             │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │  App ID: application_1234567890_0001                                │   │
  │  │  Name: MySparkJob                                                   │   │
  │  │  Duration: 2.5 hours                                                │   │
  │  │  Status: COMPLETED                                                  │   │
  │  │                                                                     │   │
  │  │  [Jobs] [Stages] [Storage] [Environment] [Executors] [SQL]         │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  │  Event logs stored in: hdfs:///spark-history/                               │
  │                        s3://bucket/spark-history/                           │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


WHY USE HISTORY SERVER?
─────────────────────────────────────────────────────────────────────────────

  Problem: Spark UI (port 4040) disappears when job completes
  Solution: History Server keeps the UI available for completed jobs

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │   Job Running ──────────────────────────> Job Completed                    │
  │                                                                             │
  │   Spark UI: localhost:4040              Spark UI: GONE!                    │
  │   (live data)                           (no way to investigate)            │
  │                                                                             │
  │                          WITH HISTORY SERVER:                              │
  │                                                                             │
  │   Job Running ──────────────────────────> Job Completed                    │
  │                                                                             │
  │   Spark UI: localhost:4040              History Server: :18080             │
  │   + Event logs written                  (loads event logs)                 │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Setting Up History Server

```
STEP-BY-STEP SETUP
═══════════════════════════════════════════════════════════════════════════════

Step 1: Configure spark-defaults.conf (on all nodes submitting jobs)
───────────────────────────────────────────────────────────────────────────
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs:///spark-history
spark.eventLog.compress          true       # Compress logs to save space

Step 2: Configure spark-history-server.conf (on history server node)
───────────────────────────────────────────────────────────────────────────
spark.history.fs.logDirectory    hdfs:///spark-history
spark.history.fs.update.interval 10s        # How often to scan for new logs
spark.history.retainedApplications 50       # Number of apps to keep in memory
spark.history.ui.port            18080
spark.history.fs.cleaner.enabled true       # Auto-delete old logs
spark.history.fs.cleaner.maxAge  7d         # Keep logs for 7 days

Step 3: Create the log directory
───────────────────────────────────────────────────────────────────────────
hdfs dfs -mkdir -p /spark-history
hdfs dfs -chmod 1777 /spark-history

Step 4: Start the History Server
───────────────────────────────────────────────────────────────────────────
$SPARK_HOME/sbin/start-history-server.sh

# Or with explicit config
$SPARK_HOME/sbin/start-history-server.sh \
  --properties-file /path/to/spark-history-server.conf


HISTORY SERVER FOR DIFFERENT STORAGE BACKENDS
─────────────────────────────────────────────────────────────────────────────

HDFS:
─────
spark.eventLog.dir                      hdfs:///spark-history
spark.history.fs.logDirectory           hdfs:///spark-history

S3:
───
spark.eventLog.dir                      s3a://my-bucket/spark-history
spark.history.fs.logDirectory           s3a://my-bucket/spark-history
spark.hadoop.fs.s3a.access.key          <access-key>
spark.hadoop.fs.s3a.secret.key          <secret-key>

Azure Blob Storage:
───────────────────
spark.eventLog.dir                      wasbs://container@account.blob.core.windows.net/spark-history
spark.history.fs.logDirectory           wasbs://container@account.blob.core.windows.net/spark-history

GCS:
────
spark.eventLog.dir                      gs://my-bucket/spark-history
spark.history.fs.logDirectory           gs://my-bucket/spark-history


ACCESSING HISTORY SERVER
─────────────────────────────────────────────────────────────────────────────

Direct Access:
http://history-server-host:18080

Via YARN ResourceManager:
1. Go to http://resourcemanager:8088
2. Click on completed application
3. Click "History" link

Via spark-submit:
--conf spark.yarn.historyServer.address=history-server:18080
```

## Debugging Spark Applications

```
DEBUGGING STRATEGIES
═══════════════════════════════════════════════════════════════════════════════

1. COLLECT LOGS
─────────────────────────────────────────────────────────────────────────────

  YARN:
  yarn logs -applicationId application_1234567890_0001 > app_logs.txt
  yarn logs -applicationId application_1234567890_0001 -containerId container_xxx

  Kubernetes:
  kubectl logs spark-driver-pod -n spark-namespace
  kubectl logs -l spark-role=executor -n spark-namespace --all-containers

  Search for common error patterns:
  grep -i "exception\|error\|failed" app_logs.txt


2. COMMON ERROR PATTERNS & SOLUTIONS
─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  ERROR                          │  LIKELY CAUSE & SOLUTION             │
  ├─────────────────────────────────┼─────────────────────────────────────┤
  │  OutOfMemoryError: Java heap    │  Increase --executor-memory         │
  │  space                          │  or reduce data per partition       │
  ├─────────────────────────────────┼─────────────────────────────────────┤
  │  OutOfMemoryError: GC overhead  │  Too much object creation           │
  │  limit exceeded                 │  Try Kryo serializer, reduce cache  │
  ├─────────────────────────────────┼─────────────────────────────────────┤
  │  Container killed by YARN for   │  Increase spark.yarn.executor       │
  │  exceeding memory limits        │  .memoryOverhead (default 10%)      │
  ├─────────────────────────────────┼─────────────────────────────────────┤
  │  SparkException: Job aborted    │  Check stage failures, data skew    │
  │  due to stage failure           │  or OOM in specific tasks           │
  ├─────────────────────────────────┼─────────────────────────────────────┤
  │  FileNotFoundException          │  Path doesn't exist or wrong perms  │
  │                                 │  Check HDFS/S3 paths and ACLs       │
  ├─────────────────────────────────┼─────────────────────────────────────┤
  │  Connection refused to driver   │  Driver died or network issue       │
  │                                 │  Check driver logs, increase memory │
  ├─────────────────────────────────┼─────────────────────────────────────┤
  │  Task not serializable          │  Closure captures non-serializable  │
  │                                 │  object, refactor code              │
  ├─────────────────────────────────┼─────────────────────────────────────┤
  │  Executor Lost                  │  Executor crashed, check container  │
  │                                 │  logs for OOM or other errors       │
  └─────────────────────────────────┴─────────────────────────────────────┘


3. SPARK UI DEBUGGING
─────────────────────────────────────────────────────────────────────────────

  Jobs Tab:
  ├── Shows all jobs triggered by actions
  ├── Red = failed, Green = succeeded
  └── Click job to see stages

  Stages Tab:
  ├── Shows shuffle read/write sizes
  ├── Task duration distribution (look for skew!)
  └── Click stage for task details

  SQL Tab (for DataFrame/SQL):
  ├── Shows logical and physical plans
  ├── Per-operator metrics
  └── Look for expensive operations (Scan, Shuffle)

  Executors Tab:
  ├── Memory usage per executor
  ├── Shuffle read/write per executor
  └── GC time (high = memory pressure)

  Storage Tab:
  ├── Cached RDDs/DataFrames
  └── Memory vs disk usage


4. ENABLE VERBOSE LOGGING
─────────────────────────────────────────────────────────────────────────────

  # In log4j2.properties (Spark 3.x)
  logger.spark.name = org.apache.spark
  logger.spark.level = DEBUG

  # For specific issues
  logger.scheduler.name = org.apache.spark.scheduler
  logger.scheduler.level = DEBUG

  logger.shuffle.name = org.apache.spark.shuffle
  logger.shuffle.level = DEBUG

  logger.storage.name = org.apache.spark.storage
  logger.storage.level = DEBUG


5. DEBUGGING DATA SKEW
─────────────────────────────────────────────────────────────────────────────

  Symptoms:
  - One task takes much longer than others
  - Stage duration = slowest task duration
  - Executor shows high shuffle read for one task

  Investigate:
  // Find skewed keys
  df.groupBy("key_column")
    .count()
    .orderBy(desc("count"))
    .show(20)

  Solutions:
  - Salting: Add random prefix to skewed keys
  - Broadcast join: For small dimension tables
  - AQE: Enable adaptive query execution
  - Repartition: Use different partitioning strategy


6. PROFILING TOOLS
─────────────────────────────────────────────────────────────────────────────

  spark-submit \
    --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
    --conf "spark.driver.extraJavaOptions=-XX:+PrintGCDetails"

  # JVM Flight Recorder (for deep profiling)
  --conf "spark.executor.extraJavaOptions=-XX:StartFlightRecording=duration=60s,filename=/tmp/executor.jfr"
```

## Optimizing Production Jobs

```
OPTIMIZATION CHECKLIST
═══════════════════════════════════════════════════════════════════════════════

1. RESOURCE OPTIMIZATION
─────────────────────────────────────────────────────────────────────────────

  ┌───────────────────────────────────────────────────────────────────────┐
  │  MEMORY OVERHEAD                                                      │
  │                                                                       │
  │  Total Container Memory = executor-memory + memoryOverhead           │
  │                                                                       │
  │  For heavy shuffle/cache:                                            │
  │  --conf spark.executor.memoryOverhead=2g  (or 15% of executor-memory)│
  │                                                                       │
  │  For PySpark:                                                        │
  │  --conf spark.executor.pyspark.memory=2g  (Python process memory)    │
  └───────────────────────────────────────────────────────────────────────┘


2. PARTITION OPTIMIZATION
─────────────────────────────────────────────────────────────────────────────

  Reading data:
  ─────────────
  # Control input partitions
  spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")
  spark.conf.set("spark.sql.files.openCostInBytes", "4m")

  Shuffle partitions:
  ───────────────────
  # Default is 200, adjust based on data size
  spark.conf.set("spark.sql.shuffle.partitions", "auto")  # AQE
  # Or explicit
  spark.conf.set("spark.sql.shuffle.partitions", "500")

  Rule of thumb:
  - Target 100-200MB per partition after shuffle
  - Too few partitions = OOM, poor parallelism
  - Too many partitions = overhead, small files


3. JOIN OPTIMIZATION
─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  JOIN TYPE           │  WHEN TO USE                                    │
  ├──────────────────────┼─────────────────────────────────────────────────┤
  │  Broadcast Hash Join │  Small table (< 10MB default) joins large table│
  │                      │  Increase: spark.sql.autoBroadcastJoinThreshold│
  │                      │  Hint: df.hint("broadcast")                     │
  ├──────────────────────┼─────────────────────────────────────────────────┤
  │  Sort Merge Join     │  Both tables large, sorted by join key         │
  │                      │  Most common for large-large joins             │
  ├──────────────────────┼─────────────────────────────────────────────────┤
  │  Shuffle Hash Join   │  Medium-large tables, unsorted                 │
  │                      │  Enable: spark.sql.join.preferSortMergeJoin=   │
  │                      │  false                                          │
  └──────────────────────┴─────────────────────────────────────────────────┘

  # Force broadcast
  val result = largeDF.join(broadcast(smallDF), "key")


4. CACHING STRATEGY
─────────────────────────────────────────────────────────────────────────────

  CACHE if dataset is:
  ✓ Used multiple times in the job
  ✓ Expensive to recompute
  ✓ Fits in memory

  DON'T CACHE if:
  ✗ Used only once
  ✗ Larger than cluster memory
  ✗ Will be filtered down significantly

  # Choose appropriate storage level
  df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Good default
  df.persist(StorageLevel.DISK_ONLY)            # Very large datasets


5. WRITE OPTIMIZATION
─────────────────────────────────────────────────────────────────────────────

  # Control output file count
  df.coalesce(10).write.parquet("output")      # Reduce to 10 files
  df.repartition(100).write.parquet("output")  # Increase to 100 files

  # Partition output by column (for query efficiency)
  df.write
    .partitionBy("year", "month")
    .parquet("output")

  # Bucketing (for repeated joins on same key)
  df.write
    .bucketBy(64, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("bucketed_table")


6. ADAPTIVE QUERY EXECUTION (AQE) - Spark 3.0+
─────────────────────────────────────────────────────────────────────────────

  # Enable all AQE features (recommended!)
  spark.sql.adaptive.enabled                    true
  spark.sql.adaptive.coalescePartitions.enabled true
  spark.sql.adaptive.skewJoin.enabled           true

  AQE automatically:
  ✓ Coalesces small shuffle partitions
  ✓ Converts sort-merge join to broadcast join at runtime
  ✓ Handles skewed joins by splitting skewed partitions


7. SPECULATION FOR STRAGGLERS
─────────────────────────────────────────────────────────────────────────────

  spark.speculation                         true
  spark.speculation.interval                100ms
  spark.speculation.multiplier              1.5    # Task > 1.5x median
  spark.speculation.quantile                0.75   # 75% tasks must complete

  Launches duplicate tasks for slow ones, uses result from whichever
  finishes first.


8. DATA FORMAT OPTIMIZATION
─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  FORMAT     │  PROS                     │  BEST FOR                    │
  ├─────────────┼───────────────────────────┼──────────────────────────────┤
  │  Parquet    │  Columnar, compressed,    │  Analytics, selective cols   │
  │             │  schema evolution         │                              │
  ├─────────────┼───────────────────────────┼──────────────────────────────┤
  │  ORC        │  Columnar, ACID support   │  Hive integration            │
  │             │  (with Hive)              │                              │
  ├─────────────┼───────────────────────────┼──────────────────────────────┤
  │  Delta Lake │  ACID, time travel,       │  Data lakes, streaming       │
  │             │  upserts                  │                              │
  ├─────────────┼───────────────────────────┼──────────────────────────────┤
  │  Avro       │  Row-based, schema        │  Streaming, Kafka            │
  │             │  evolution                │                              │
  └─────────────┴───────────────────────────┴──────────────────────────────┘

  Always prefer columnar formats (Parquet/ORC) for analytics!
```

## Performance Monitoring Metrics

```
KEY METRICS TO WATCH
═══════════════════════════════════════════════════════════════════════════════

SPARK UI METRICS:
─────────────────────────────────────────────────────────────────────────────
┌─────────────────────┬─────────────────────────────────────────────────────┐
│  Metric             │  What it tells you                                  │
├─────────────────────┼─────────────────────────────────────────────────────┤
│  Scheduler Delay    │  Time to schedule tasks (high = too many tasks)    │
│  Task Duration      │  Time per task (look for outliers = skew)          │
│  GC Time            │  Time spent in GC (>10% = memory pressure)         │
│  Shuffle Read/Write │  Data movement (high = expensive shuffles)         │
│  Peak Memory        │  Memory usage per executor                          │
│  Spill (Disk)       │  Data spilled to disk (increase executor memory)   │
└─────────────────────┴─────────────────────────────────────────────────────┘


CLUSTER METRICS:
─────────────────────────────────────────────────────────────────────────────
- CPU utilization per executor (low = not enough tasks)
- Memory utilization (OOM risk)
- Network I/O (shuffle bound jobs)
- Disk I/O (spill to disk, HDFS reads)


METRICS SINKS (for external monitoring):
─────────────────────────────────────────────────────────────────────────────
# Enable metrics reporting
spark.metrics.conf.*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
spark.metrics.conf.*.sink.graphite.host=graphite-host
spark.metrics.conf.*.sink.graphite.port=2003

# Or Prometheus
spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus
```

## Common Production Configurations

```
spark-defaults.conf
═══════════════════════════════════════════════════════════════════════════════

# Serialization
spark.serializer                          org.apache.spark.serializer.KryoSerializer

# Memory
spark.memory.fraction                     0.6
spark.memory.storageFraction              0.5

# Shuffle
spark.sql.shuffle.partitions              200
spark.shuffle.compress                    true
spark.shuffle.spill.compress              true

# Network
spark.network.timeout                     800s
spark.executor.heartbeatInterval          60s

# Dynamic allocation (with YARN)
spark.dynamicAllocation.enabled           true
spark.dynamicAllocation.minExecutors      2
spark.dynamicAllocation.maxExecutors      100
spark.shuffle.service.enabled             true

# Adaptive Query Execution (Spark 3+)
spark.sql.adaptive.enabled                true
spark.sql.adaptive.coalescePartitions.enabled  true

# Event logging for History Server
spark.eventLog.enabled                    true
spark.eventLog.dir                        hdfs:///spark-history

# Speculation for stragglers
spark.speculation                         true
spark.speculation.multiplier              1.5
```

## Instructions

1. **Read** this README - understand debugging and optimization techniques
2. **Review** `src/main/scala/Exercise.scala` - practice debugging scenarios
3. **Run**: `cd kata-22-history-server-debugging && sbt run`
4. **Explore** Spark UI at http://localhost:4040 while running

## Key Takeaways

1. **History Server** - Essential for post-mortem analysis of completed jobs
2. **Event logging** - Enable with `spark.eventLog.enabled=true`
3. **Log collection** - `yarn logs -applicationId` for YARN logs
4. **Common errors** - OOM, container killed, task not serializable
5. **Spark UI** - Jobs, Stages, SQL tabs for debugging
6. **Data skew** - Look for long-running tasks, use salting or AQE
7. **AQE** - Enable for automatic runtime optimization
8. **Partitions** - Target 100-200MB per partition
9. **Caching** - Only cache if reused multiple times
10. **Monitoring** - Track GC time, shuffle, spill metrics

## Time
~35 minutes

## Congratulations!
You've completed all 22 Spark Katas covering everything from basics to production deployment and debugging!
