# Kata 20: Performance Tuning

## Goal
Master the art of optimizing Spark applications for production workloads.

## The Performance Tuning Mindset

```
TUNING HIERARCHY (Start from top!)
═══════════════════════════════════════════════════════════════════════════════

    1. DATA LEVEL        ← Biggest impact, tune first!
       └── Data format (Parquet vs CSV)
       └── Partitioning strategy
       └── Data skew handling

    2. APPLICATION LEVEL
       └── Shuffle optimization
       └── Join strategies
       └── Caching strategy

    3. CLUSTER LEVEL
       └── Executor sizing
       └── Memory configuration
       └── Parallelism settings

    4. JVM LEVEL         ← Smallest impact, tune last
       └── GC tuning
       └── Off-heap memory

    Don't tune JVM before fixing data skew!
```

## Data Format Comparison

```
FORMAT PERFORMANCE COMPARISON (Reading 1TB data)
═══════════════════════════════════════════════════════════════════════════════

    ┌──────────────┬────────────┬────────────┬──────────────┬────────────────┐
    │   Format     │  Size on   │  Read Time │  Predicate   │  Column        │
    │              │   Disk     │            │  Pushdown    │  Pruning       │
    ├──────────────┼────────────┼────────────┼──────────────┼────────────────┤
    │  CSV         │   1 TB     │   60 min   │     No       │     No         │
    │  JSON        │   1.2 TB   │   90 min   │     No       │     No         │
    │  Avro        │   400 GB   │   20 min   │     No       │     Yes        │
    │  ORC         │   250 GB   │   8 min    │     Yes      │     Yes        │
    │  Parquet     │   250 GB   │   8 min    │     Yes      │     Yes        │
    │  Delta Lake  │   250 GB   │   7 min    │     Yes      │     Yes        │
    └──────────────┴────────────┴────────────┴──────────────┴────────────────┘

    Parquet/ORC: 75% smaller, 7x faster than CSV!


PARQUET OPTIMIZATIONS:
─────────────────────────────────────────────────────────────────────────────
    // Optimize write
    df.write
      .option("compression", "snappy")     // Fast compression
      .option("parquet.block.size", 128*1024*1024)  // 128MB row groups
      .parquet("output")

    // Partition by high-cardinality columns
    df.write
      .partitionBy("year", "month")        // Creates year=2024/month=01/
      .parquet("output")

    // Sort within partitions for better compression
    df.sortWithinPartitions("timestamp")
      .write.parquet("output")
```

## Partition Strategy

```
PARTITION SIZING GUIDELINES
═══════════════════════════════════════════════════════════════════════════════

    TOO FEW PARTITIONS:
    ┌────────────────────────────────────────────────────────────────────────┐
    │  4 partitions for 100GB data = 25GB per partition                     │
    │                                                                        │
    │  Problems:                                                            │
    │  - Not enough parallelism (underutilized cluster)                    │
    │  - OOM errors (partition doesn't fit in memory)                      │
    │  - Long GC pauses                                                    │
    │  - Slow tasks (processing 25GB takes time)                           │
    └────────────────────────────────────────────────────────────────────────┘


    TOO MANY PARTITIONS:
    ┌────────────────────────────────────────────────────────────────────────┐
    │  100,000 partitions for 100GB = 1MB per partition                     │
    │                                                                        │
    │  Problems:                                                            │
    │  - Task scheduling overhead                                           │
    │  - Small file problem                                                 │
    │  - Driver memory pressure (tracking 100K tasks)                       │
    │  - Inefficient I/O (seek time > read time)                           │
    └────────────────────────────────────────────────────────────────────────┘


    OPTIMAL:
    ┌────────────────────────────────────────────────────────────────────────┐
    │  Target: 100-200MB per partition                                      │
    │                                                                        │
    │  For 100GB data: 500-1000 partitions                                 │
    │                                                                        │
    │  Formula:                                                             │
    │  partitions = max(data_size / 128MB, 2 * total_cores)                │
    └────────────────────────────────────────────────────────────────────────┘
```

## Data Skew: The Silent Killer

```
DETECTING DATA SKEW
═══════════════════════════════════════════════════════════════════════════════

    Symptoms in Spark UI:
    - One task takes 10x longer than others
    - One task processes 100x more data than others
    - Job stuck at 99% for a long time

    ┌──────────────────────────────────────────────────────────────────────┐
    │  Task Distribution (Spark UI → Stages → Event Timeline)             │
    │                                                                      │
    │  Task 1:  ████ (2s)                                                 │
    │  Task 2:  ████ (2s)                                                 │
    │  Task 3:  ████████████████████████████████████████████ (100s) ←SKEW │
    │  Task 4:  ████ (2s)                                                 │
    │                                                                      │
    │  Total job time = slowest task = 100s!                              │
    └──────────────────────────────────────────────────────────────────────┘


COMMON CAUSES:
─────────────────────────────────────────────────────────────────────────────
    - Null values in join key
    - Popular values (e.g., "USA" in country field)
    - Default/placeholder values
    - Hot keys in aggregations


SOLUTIONS:
─────────────────────────────────────────────────────────────────────────────

    1. SALTING (Add random prefix to spread key across partitions)

       // Before: All "USA" goes to one partition
       // After: "USA_0", "USA_1", "USA_2" spread across 3 partitions

       val saltedDF = df.withColumn("salted_key",
         concat($"country", lit("_"), (rand() * 10).cast("int")))

       val result = saltedDF
         .groupBy("salted_key")
         .agg(sum("amount"))
         .withColumn("country", split($"salted_key", "_")(0))
         .groupBy("country")
         .agg(sum("sum(amount)"))


    2. BROADCAST SMALL TABLE (Avoid shuffle entirely)

       // If one side of join is small, broadcast it
       val result = largeDF.join(broadcast(smallDF), "key")


    3. ADAPTIVE QUERY EXECUTION (Spark 3.x)

       spark.conf.set("spark.sql.adaptive.enabled", "true")
       spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")


    4. SEPARATE SKEWED KEYS

       // Handle hot keys separately
       val hotKeys = Set("USA", "China", "India")

       val hotDF = df.filter($"country".isin(hotKeys: _*))
       val normalDF = df.filter(!$"country".isin(hotKeys: _*))

       val hotResult = hotDF.repartition(100, $"country")...
       val normalResult = normalDF...

       hotResult.union(normalResult)
```

## Memory Configuration

```
EXECUTOR MEMORY BREAKDOWN
═══════════════════════════════════════════════════════════════════════════════

    spark.executor.memory = 8g

    ┌─────────────────────────────────────────────────────────────────────────┐
    │                                                                         │
    │  Reserved Memory: 300MB (fixed overhead)                               │
    │  ┌─────────────────────────────────────────────────────────────────┐   │
    │  └─────────────────────────────────────────────────────────────────┘   │
    │                                                                         │
    │  Usable Memory: 7.7GB (8GB - 300MB)                                    │
    │                                                                         │
    │  Spark Memory (60% = 4.6GB)                                            │
    │  ┌─────────────────────────────────────────────────────────────────┐   │
    │  │  ┌────────────────────┬──────────────────────────────────────┐ │   │
    │  │  │  Storage (50%)     │  Execution (50%)                     │ │   │
    │  │  │  2.3GB             │  2.3GB                               │ │   │
    │  │  │                    │                                      │ │   │
    │  │  │  - Cached RDDs     │  - Shuffles                         │ │   │
    │  │  │  - Broadcast vars  │  - Joins                            │ │   │
    │  │  │                    │  - Sorts                            │ │   │
    │  │  │                    │  - Aggregations                     │ │   │
    │  │  └────────────────────┴──────────────────────────────────────┘ │   │
    │  │             ↑ Unified: can borrow from each other ↑            │   │
    │  └─────────────────────────────────────────────────────────────────┘   │
    │                                                                         │
    │  User Memory (40% = 3.1GB)                                             │
    │  ┌─────────────────────────────────────────────────────────────────┐   │
    │  │  - User data structures                                         │   │
    │  │  - UDF objects                                                  │   │
    │  │  - Internal metadata                                            │   │
    │  └─────────────────────────────────────────────────────────────────┘   │
    │                                                                         │
    └─────────────────────────────────────────────────────────────────────────┘


MEMORY TUNING:
─────────────────────────────────────────────────────────────────────────────
    # Increase storage fraction for caching-heavy workloads
    spark.memory.storageFraction = 0.6

    # Increase executor memory for large shuffles
    spark.executor.memory = 16g

    # Add off-heap memory for very large datasets
    spark.memory.offHeap.enabled = true
    spark.memory.offHeap.size = 4g
```

## Shuffle Optimization

```
SHUFFLE PERFORMANCE FACTORS
═══════════════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────────────────────┐
    │  MAP SIDE (Writing shuffle files)                                      │
    │  ─────────────────────────────────────────────────────────────────────  │
    │                                                                         │
    │  spark.shuffle.file.buffer = 32k      # Buffer for writing shuffle     │
    │  spark.shuffle.spill.batchSize = 10000 # Records before spilling       │
    │                                                                         │
    │  If map tasks are slow writing shuffle:                                │
    │  - Increase buffer size                                                │
    │  - Add more local disk for spill                                       │
    └─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Network Transfer
                                    ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  REDUCE SIDE (Reading shuffle files)                                   │
    │  ─────────────────────────────────────────────────────────────────────  │
    │                                                                         │
    │  spark.reducer.maxSizeInFlight = 48m  # Max data in flight per reducer │
    │  spark.shuffle.compress = true         # Compress shuffle output       │
    │  spark.shuffle.io.maxRetries = 3       # Retry failed fetches          │
    │                                                                         │
    │  If reduce tasks are slow fetching:                                    │
    │  - Increase maxSizeInFlight                                           │
    │  - Check network bandwidth                                            │
    │  - Enable compression                                                  │
    └─────────────────────────────────────────────────────────────────────────┘


SHUFFLE PARTITION TUNING:
─────────────────────────────────────────────────────────────────────────────
    spark.sql.shuffle.partitions = 200      # Default, often too low/high

    # Rule of thumb:
    # partitions = shuffle_data_size / 100MB

    # For 50GB shuffle: 500 partitions
    # For 500GB shuffle: 5000 partitions

    # With AQE, Spark auto-tunes this:
    spark.sql.adaptive.enabled = true
    spark.sql.adaptive.coalescePartitions.enabled = true
```

## Join Optimization

```
JOIN STRATEGY SELECTION
═══════════════════════════════════════════════════════════════════════════════

    ┌─────────────────┬─────────────────────────────────────────────────────┐
    │  Small + Large  │  BROADCAST JOIN (Best!)                             │
    │  (< 10MB)       │  - No shuffle                                       │
    │                 │  - Small table sent to all executors                │
    │                 │  - broadcast(smallDF).join(largeDF, "key")          │
    ├─────────────────┼─────────────────────────────────────────────────────┤
    │  Medium + Large │  SHUFFLE HASH JOIN                                  │
    │  (10MB - 1GB)   │  - Both tables shuffled by key                      │
    │                 │  - Hash table built for smaller side                │
    │                 │  - spark.sql.join.preferSortMergeJoin = false       │
    ├─────────────────┼─────────────────────────────────────────────────────┤
    │  Large + Large  │  SORT MERGE JOIN (Default)                          │
    │  (> 1GB each)   │  - Both tables sorted by key                        │
    │                 │  - Merge sorted streams                             │
    │                 │  - Handles any size data                            │
    └─────────────────┴─────────────────────────────────────────────────────┘


OPTIMIZING JOINS:
─────────────────────────────────────────────────────────────────────────────
    // 1. Broadcast small tables
    spark.sql.autoBroadcastJoinThreshold = 100MB  // Increase threshold

    // 2. Pre-partition tables for repeated joins
    val partitionedOrders = orders.repartition($"customer_id")
    val partitionedCustomers = customers.repartition($"id")
    // Subsequent joins on these keys won't shuffle!

    // 3. Use bucketing for very large repeated joins
    orders.write.bucketBy(100, "customer_id").saveAsTable("orders_bucketed")
    customers.write.bucketBy(100, "id").saveAsTable("customers_bucketed")
    // Bucket-to-bucket joins: no shuffle!
```

## Performance Checklist

```
PRE-FLIGHT CHECKLIST FOR PRODUCTION JOBS
═══════════════════════════════════════════════════════════════════════════════

DATA:
  □ Using columnar format (Parquet/ORC)?
  □ Data partitioned appropriately?
  □ No data skew?
  □ Predicate pushdown working?

SHUFFLES:
  □ Minimized number of shuffles?
  □ Using reduceByKey instead of groupByKey?
  □ Broadcast joins where applicable?
  □ shuffle.partitions tuned?

MEMORY:
  □ Executor memory sized correctly?
  □ Caching strategy in place?
  □ No memory spills (check Spark UI)?
  □ GC time < 10% of task time?

PARALLELISM:
  □ Partition count = 2-4x cores?
  □ No single long-running task?
  □ AQE enabled (Spark 3.x)?

SERIALIZATION:
  □ Using Kryo serializer?
  □ Classes registered?
  □ No NotSerializableException?
```

## Spark UI Deep Dive

```
WHERE TO LOOK IN SPARK UI
═══════════════════════════════════════════════════════════════════════════════

JOBS TAB:
  - Overall job progress
  - Failed stages (click for details)

STAGES TAB:
  - Shuffle read/write sizes (should be balanced)
  - Task distribution (look for outliers)
  - Input/Output sizes

STORAGE TAB:
  - Cached RDDs
  - Memory usage per executor

EXECUTORS TAB:
  - GC time (should be < 10%)
  - Shuffle spill (should be 0 ideally)
  - Task time distribution

SQL TAB:
  - Query plans
  - Physical plan details
  - Metrics per operator
```

## Instructions

1. **Read** this README - understand the tuning hierarchy
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-20-performance-tuning && sbt run`
4. **Monitor** Spark UI at http://localhost:4040
5. **Check** `solution/Solution.scala` if needed

## Quick Reference: Key Configurations

| Category | Config | Recommended |
|----------|--------|-------------|
| Memory | `spark.executor.memory` | 4-16g |
| Memory | `spark.memory.fraction` | 0.6 |
| Shuffle | `spark.sql.shuffle.partitions` | data_size/100MB |
| Join | `spark.sql.autoBroadcastJoinThreshold` | 10-100MB |
| Adaptive | `spark.sql.adaptive.enabled` | true |
| Serialization | `spark.serializer` | KryoSerializer |

## Time
~40 minutes

## Congratulations!
You've completed all 20 Spark Katas! You now have a deep understanding of:
- Spark's execution model (RDDs, DAG, stages, tasks)
- Memory management and caching
- Shuffle optimization
- Query optimization (Catalyst, Tungsten)
- Performance tuning strategies

Keep practicing and refer back to these katas when debugging production issues!
