# Kata 13: Memory Management

## Goal
Understand Spark's unified memory model and how to tune it for optimal performance.
Learn about storage vs execution memory, off-heap memory, garbage collection tuning,
and how to diagnose and fix memory issues.

## Why Memory Management Matters

Memory is often the limiting factor in Spark applications. Understanding how Spark uses
memory helps you avoid OOM errors, reduce GC pauses, and maximize throughput.

```
═══════════════════════════════════════════════════════════════════════════════════════
                      MEMORY CHALLENGES IN SPARK
═══════════════════════════════════════════════════════════════════════════════════════

  COMMON MEMORY PROBLEMS:

  1. OUT OF MEMORY (OOM)
     ┌─────────────────────────────────────────────────────────────────────────────────┐
     │  java.lang.OutOfMemoryError: Java heap space                                    │
     │  • Executor ran out of memory during computation                                │
     │  • Usually during shuffle, sort, or aggregation                                 │
     └─────────────────────────────────────────────────────────────────────────────────┘

  2. EXCESSIVE GARBAGE COLLECTION
     ┌─────────────────────────────────────────────────────────────────────────────────┐
     │  "GC overhead limit exceeded"                                                   │
     │  • JVM spending >98% time on GC                                                 │
     │  • Application barely making progress                                           │
     │  • Typical sign: tasks taking 10x longer than expected                          │
     └─────────────────────────────────────────────────────────────────────────────────┘

  3. DISK SPILLS
     ┌─────────────────────────────────────────────────────────────────────────────────┐
     │  "Spilling data to disk"                                                        │
     │  • Not enough execution memory                                                  │
     │  • 10-100x slower than in-memory operations                                     │
     │  • Watch for: shuffle spill metrics in Spark UI                                 │
     └─────────────────────────────────────────────────────────────────────────────────┘

  4. CONTAINER KILLED BY YARN/K8S
     ┌─────────────────────────────────────────────────────────────────────────────────┐
     │  "Container killed by YARN for exceeding memory limits"                         │
     │  • Process used more memory than allocated                                      │
     │  • Often due to off-heap memory not accounted for                               │
     └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Spark Memory Architecture

Spark uses a unified memory management model that dynamically shares memory between
storage (caching) and execution (computation).

```
═══════════════════════════════════════════════════════════════════════════════════════
                         EXECUTOR MEMORY LAYOUT
═══════════════════════════════════════════════════════════════════════════════════════

  Total JVM Heap (spark.executor.memory = 4g)
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │                     RESERVED MEMORY (300 MB)                               │ │
  │  │                     Fixed overhead for Spark internals                     │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │                                                                           │ │
  │  │                     SPARK MEMORY                                          │ │
  │  │                     (heap - 300MB) * spark.memory.fraction                │ │
  │  │                     Default: (4096MB - 300MB) * 0.6 = 2278 MB             │ │
  │  │                                                                           │ │
  │  │  ┌───────────────────────────┬─────────────────────────────────────────┐ │ │
  │  │  │                           │                                         │ │ │
  │  │  │    STORAGE MEMORY         │         EXECUTION MEMORY                │ │ │
  │  │  │                           │                                         │ │ │
  │  │  │  - Cached RDDs/DFs        │  - Shuffles                             │ │ │
  │  │  │  - Broadcast variables    │  - Joins                                │ │ │
  │  │  │  - Accumulator results    │  - Sorts                                │ │ │
  │  │  │                           │  - Aggregations                         │ │ │
  │  │  │                           │                                         │ │ │
  │  │  │  ◄── storageFraction ──►  │  ◄── (1 - storageFraction) ──►          │ │ │
  │  │  │       (default 0.5)       │         (default 0.5)                   │ │ │
  │  │  │                           │                                         │ │ │
  │  │  │         1139 MB           │            1139 MB                      │ │ │
  │  │  │                           │                                         │ │ │
  │  │  └───────────────────────────┴─────────────────────────────────────────┘ │ │
  │  │                                                                           │ │
  │  │                    ↑↓ UNIFIED: Can borrow from each other ↑↓              │ │
  │  │                                                                           │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │                     USER MEMORY                                            │ │
  │  │                     (heap - 300MB) * (1 - spark.memory.fraction)          │ │
  │  │                     Default: (4096MB - 300MB) * 0.4 = 1518 MB              │ │
  │  │                                                                           │ │
  │  │  - User data structures in RDD transformations                            │ │
  │  │  - Internal Spark metadata                                                │ │
  │  │  - UDF execution overhead                                                 │ │
  │  │  - Temporary objects created during task execution                        │ │
  │  │                                                                           │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## The Unified Memory Model

The key insight of Spark's unified memory model is that storage and execution memory
can borrow from each other dynamically.

```
═══════════════════════════════════════════════════════════════════════════════════════
                         UNIFIED MEMORY MODEL
═══════════════════════════════════════════════════════════════════════════════════════

  SCENARIO 1: Heavy caching, light execution
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  SPARK MEMORY (2278 MB)                                                         │
  │  ┌─────────────────────────────────────────────┬─────────────────────────────┐ │
  │  │                                             │                             │ │
  │  │           STORAGE MEMORY                    │     EXECUTION MEMORY        │ │
  │  │           (using 1800 MB)                   │     (using 400 MB)          │ │
  │  │                                             │                             │ │
  │  │  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓   │  ░░░░░░░░░░░░░░░░           │ │
  │  │                                             │                             │ │
  │  │  Storage borrowed from execution!           │  Execution has room         │ │
  │  │                                             │                             │ │
  │  └─────────────────────────────────────────────┴─────────────────────────────┘ │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  SCENARIO 2: Light caching, heavy execution (shuffle)
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  SPARK MEMORY (2278 MB)                                                         │
  │  ┌─────────────────────────────┬─────────────────────────────────────────────┐ │
  │  │                             │                                             │ │
  │  │    STORAGE MEMORY           │            EXECUTION MEMORY                 │ │
  │  │    (using 500 MB)           │            (using 1700 MB)                  │ │
  │  │                             │                                             │ │
  │  │  ░░░░░░░░░░░░░░             │  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ │ │
  │  │                             │                                             │ │
  │  │  Storage shrunk             │  Execution borrowed from storage!           │ │
  │  │  (may evict cached data)    │                                             │ │
  │  │                             │                                             │ │
  │  └─────────────────────────────┴─────────────────────────────────────────────┘ │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  KEY RULES:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  1. EXECUTION CAN EVICT STORAGE:                                                │
  │     When execution needs more memory, it can evict cached data                  │
  │     (only the portion borrowed from storage, not the guaranteed storage)        │
  │                                                                                 │
  │  2. STORAGE CANNOT EVICT EXECUTION:                                             │
  │     Storage cannot reclaim memory currently used by execution                   │
  │     (execution has higher priority - must complete tasks)                       │
  │                                                                                 │
  │  3. GUARANTEED MINIMUMS:                                                        │
  │     - Storage: spark.memory.storageFraction * sparkMemory                       │
  │     - Execution: (1 - spark.memory.storageFraction) * sparkMemory               │
  │                                                                                 │
  │  4. DYNAMIC SHARING:                                                            │
  │     Both can use more than their minimum if the other isn't using it            │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Memory Calculation Example

```
═══════════════════════════════════════════════════════════════════════════════════════
                      MEMORY CALCULATION EXAMPLE
═══════════════════════════════════════════════════════════════════════════════════════

  Configuration:
  - spark.executor.memory = 8g (8192 MB)
  - spark.memory.fraction = 0.6
  - spark.memory.storageFraction = 0.5

  Calculation:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Total Heap                    = 8192 MB                                        │
  │                                                                                 │
  │  Reserved Memory               = 300 MB                                         │
  │                                                                                 │
  │  Usable Memory                 = 8192 - 300 = 7892 MB                           │
  │                                                                                 │
  │  Spark Memory                  = 7892 × 0.6 = 4735 MB                           │
  │    ├─ Storage Memory (initial) = 4735 × 0.5 = 2368 MB                           │
  │    └─ Execution Memory (init)  = 4735 × 0.5 = 2368 MB                           │
  │                                                                                 │
  │  User Memory                   = 7892 × 0.4 = 3157 MB                           │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  VISUAL BREAKDOWN:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                        Total Heap: 8192 MB                                      │
  ├────────┬────────────────────────────────────────────────┬───────────────────────┤
  │Reserved│              Spark Memory (4735 MB)            │ User Memory (3157 MB) │
  │ 300 MB │ ┌─────────────────┬────────────────────────┐  │                       │
  │        │ │ Storage 2368 MB │ Execution 2368 MB      │  │                       │
  │        │ │    (cached)     │   (shuffle/sort)       │  │                       │
  │        │ └─────────────────┴────────────────────────┘  │                       │
  └────────┴───────────────────────────────────────────────┴───────────────────────┘
  | 3.7%   |                    57.8%                       |        38.5%         |

═══════════════════════════════════════════════════════════════════════════════════════
```

## Storage Memory Deep Dive

Storage memory is used for caching data and broadcast variables.

```
═══════════════════════════════════════════════════════════════════════════════════════
                          STORAGE MEMORY
═══════════════════════════════════════════════════════════════════════════════════════

  WHAT USES STORAGE MEMORY:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  1. CACHED RDDs/DataFrames                                                      │
  │     df.cache()                                                                  │
  │     df.persist(StorageLevel.MEMORY_ONLY)                                        │
  │                                                                                 │
  │  2. BROADCAST VARIABLES                                                         │
  │     val bc = spark.sparkContext.broadcast(largeMap)                             │
  │     broadcast(smallDF) in joins                                                 │
  │                                                                                 │
  │  3. SHUFFLE BLOCKS (for reading)                                                │
  │     Shuffle data read from other executors                                      │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  STORAGE LEVELS:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Level                    │ Memory │ Disk │ Serialized │ Replication │         │
  │  ─────────────────────────┼────────┼──────┼────────────┼─────────────┤         │
  │  MEMORY_ONLY              │   ✓    │      │            │     1       │ Default │
  │  MEMORY_ONLY_SER          │   ✓    │      │     ✓      │     1       │         │
  │  MEMORY_AND_DISK          │   ✓    │  ✓   │            │     1       │         │
  │  MEMORY_AND_DISK_SER      │   ✓    │  ✓   │     ✓      │     1       │         │
  │  DISK_ONLY                │        │  ✓   │            │     1       │         │
  │  MEMORY_ONLY_2            │   ✓    │      │            │     2       │         │
  │  OFF_HEAP                 │ Off-HP │      │     ✓      │     1       │         │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  MEMORY_ONLY vs MEMORY_ONLY_SER:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  MEMORY_ONLY (Deserialized):                                                    │
  │  ┌────────────────────────────────────────────────────────────────────────┐    │
  │  │  [Object] [Object] [Object] [Object] [Object] [Object]                 │    │
  │  │                                                                        │    │
  │  │  + Fast access (objects ready to use)                                  │    │
  │  │  + Good for iterative algorithms                                       │    │
  │  │  - High memory overhead (object headers, pointers)                     │    │
  │  │  - Can cause GC pressure                                               │    │
  │  └────────────────────────────────────────────────────────────────────────┘    │
  │                                                                                 │
  │  MEMORY_ONLY_SER (Serialized):                                                  │
  │  ┌────────────────────────────────────────────────────────────────────────┐    │
  │  │  [byte array with all objects serialized together]                     │    │
  │  │                                                                        │    │
  │  │  + 2-5x less memory (no object overhead)                               │    │
  │  │  + Less GC pressure                                                    │    │
  │  │  - Slower access (must deserialize)                                    │    │
  │  │  - CPU overhead for serialization                                      │    │
  │  └────────────────────────────────────────────────────────────────────────┘    │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Execution Memory Deep Dive

Execution memory is used for computation - shuffles, sorts, joins, and aggregations.

```
═══════════════════════════════════════════════════════════════════════════════════════
                         EXECUTION MEMORY
═══════════════════════════════════════════════════════════════════════════════════════

  WHAT USES EXECUTION MEMORY:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  1. SHUFFLES                                                                    │
  │     ┌────────────────────────────────────────────────────────────────────┐     │
  │     │  df.groupBy("key").count()                                          │     │
  │     │  df.repartition(100)                                                │     │
  │     │                                                                     │     │
  │     │  Execution memory used for:                                         │     │
  │     │  - Shuffle write buffers                                            │     │
  │     │  - Shuffle read aggregation                                         │     │
  │     │  - Spill buffers when memory is full                                │     │
  │     └────────────────────────────────────────────────────────────────────┘     │
  │                                                                                 │
  │  2. JOINS                                                                       │
  │     ┌────────────────────────────────────────────────────────────────────┐     │
  │     │  df1.join(df2, "key")                                               │     │
  │     │                                                                     │     │
  │     │  Memory used for:                                                   │     │
  │     │  - Hash tables (in hash joins)                                      │     │
  │     │  - Sort buffers (in sort-merge joins)                               │     │
  │     │  - Broadcast data (loaded into execution memory first)              │     │
  │     └────────────────────────────────────────────────────────────────────┘     │
  │                                                                                 │
  │  3. SORTS                                                                       │
  │     ┌────────────────────────────────────────────────────────────────────┐     │
  │     │  df.orderBy("column")                                               │     │
  │     │  df.sortWithinPartitions("column")                                  │     │
  │     │                                                                     │     │
  │     │  Memory used for:                                                   │     │
  │     │  - In-memory sort buffers                                           │     │
  │     │  - Spill files when buffer full                                     │     │
  │     └────────────────────────────────────────────────────────────────────┘     │
  │                                                                                 │
  │  4. AGGREGATIONS                                                                │
  │     ┌────────────────────────────────────────────────────────────────────┐     │
  │     │  df.groupBy("key").agg(sum("value"))                                │     │
  │     │                                                                     │     │
  │     │  Memory used for:                                                   │     │
  │     │  - Hash maps for aggregation                                        │     │
  │     │  - Partial aggregation buffers                                      │     │
  │     └────────────────────────────────────────────────────────────────────┘     │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  SPILLING TO DISK:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  When execution memory is exhausted, Spark spills to disk:                      │
  │                                                                                 │
  │  ┌────────────────────────────────────────────────────────────────────────┐    │
  │  │                                                                        │    │
  │  │  In-Memory Buffer          Disk                                        │    │
  │  │  ┌──────────────┐         ┌──────────────────────────────────────────┐│    │
  │  │  │ Sort buffer  │─────────│ spill_file_1 │ spill_file_2 │ ...       ││    │
  │  │  │ (full!)      │  Spill  └──────────────────────────────────────────┘│    │
  │  │  └──────────────┘                                                      │    │
  │  │                                                                        │    │
  │  │  Spilling is 10-100x slower than in-memory!                            │    │
  │  │  Check Spark UI → Stage → Shuffle Spill (Memory/Disk)                  │    │
  │  │                                                                        │    │
  │  └────────────────────────────────────────────────────────────────────────┘    │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Off-Heap Memory

Off-heap memory is allocated outside the JVM heap, avoiding GC overhead.

```
═══════════════════════════════════════════════════════════════════════════════════════
                           OFF-HEAP MEMORY
═══════════════════════════════════════════════════════════════════════════════════════

  TOTAL EXECUTOR MEMORY:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │                        JVM HEAP (On-Heap)                                  │ │
  │  │                        spark.executor.memory                               │ │
  │  │  ┌─────────────────────────────────────────────────────────────────────┐  │ │
  │  │  │                                                                     │  │ │
  │  │  │  Spark Memory │ User Memory │ Reserved                              │  │ │
  │  │  │  (Storage +   │             │                                       │  │ │
  │  │  │   Execution)  │             │                                       │  │ │
  │  │  │               │             │                                       │  │ │
  │  │  └─────────────────────────────────────────────────────────────────────┘  │ │
  │  │                        Subject to GC                                       │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │                        OFF-HEAP MEMORY                                     │ │
  │  │                        spark.memory.offHeap.size                           │ │
  │  │  ┌─────────────────────────────────────────────────────────────────────┐  │ │
  │  │  │                                                                     │  │ │
  │  │  │  Direct Memory (UnsafeRow, Tungsten operations)                     │  │ │
  │  │  │                                                                     │  │ │
  │  │  │  + No GC overhead                                                   │  │ │
  │  │  │  + Faster for some operations                                       │  │ │
  │  │  │  + More predictable latency                                         │  │ │
  │  │  │                                                                     │  │ │
  │  │  └─────────────────────────────────────────────────────────────────────┘  │ │
  │  │                        NOT subject to GC                                   │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │                        MEMORY OVERHEAD                                     │ │
  │  │                        spark.executor.memoryOverhead                       │ │
  │  │  ┌─────────────────────────────────────────────────────────────────────┐  │ │
  │  │  │                                                                     │  │ │
  │  │  │  - VM overhead                                                      │  │ │
  │  │  │  - Interned strings                                                 │  │ │
  │  │  │  - Other native overhead                                            │  │ │
  │  │  │  - PySpark/SparkR memory                                            │  │ │
  │  │  │                                                                     │  │ │
  │  │  │  Default: max(384MB, 10% of executor memory)                        │  │ │
  │  │  │                                                                     │  │ │
  │  │  └─────────────────────────────────────────────────────────────────────┘  │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Key Configuration Parameters

```scala
// ============================================
// EXECUTOR MEMORY
// ============================================

// Total heap memory per executor (most important setting)
spark.conf.set("spark.executor.memory", "8g")

// Memory overhead for non-heap usage (default: max(384MB, 10% of executor memory))
spark.conf.set("spark.executor.memoryOverhead", "1g")

// ============================================
// SPARK MEMORY FRACTION
// ============================================

// Fraction of (heap - 300MB) for Spark execution/storage (default: 0.6)
spark.conf.set("spark.memory.fraction", "0.6")

// Fraction of Spark memory for storage (default: 0.5)
spark.conf.set("spark.memory.storageFraction", "0.5")

// ============================================
// OFF-HEAP MEMORY
// ============================================

// Enable off-heap memory
spark.conf.set("spark.memory.offHeap.enabled", "true")

// Off-heap memory size (must be set if enabled)
spark.conf.set("spark.memory.offHeap.size", "2g")

// ============================================
// DRIVER MEMORY
// ============================================

// Driver memory (for collect(), broadcast, etc.)
spark.conf.set("spark.driver.memory", "4g")

// Driver memory overhead
spark.conf.set("spark.driver.memoryOverhead", "512m")

// ============================================
// SHUFFLE MEMORY
// ============================================

// Number of shuffle partitions (affects memory per partition)
spark.conf.set("spark.sql.shuffle.partitions", "200")

// Initial buffer size for shuffle (per task)
spark.conf.set("spark.shuffle.file.buffer", "32k")

// Sort buffer for shuffle (default: 4MB per task)
spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "200")
```

## Memory Tuning Guidelines

```
═══════════════════════════════════════════════════════════════════════════════════════
                         MEMORY TUNING GUIDELINES
═══════════════════════════════════════════════════════════════════════════════════════

  RULE OF THUMB FOR EXECUTOR SIZING:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Executor Memory: 4-8 GB per executor is often optimal                          │
  │  - Too small: Not enough for shuffle/sort operations                            │
  │  - Too large: Long GC pauses, YARN container limits                             │
  │                                                                                 │
  │  Executor Cores: 4-5 cores per executor                                         │
  │  - Memory per core: 1-2 GB minimum                                              │
  │  - More cores = more concurrent tasks = more memory contention                  │
  │                                                                                 │
  │  EXAMPLE CONFIGURATIONS:                                                        │
  │  ┌────────────────────────────────────────────────────────────────────────┐    │
  │  │  Small cluster (testing):                                              │    │
  │  │  - 2 executors × 4 GB × 2 cores = 16 GB total                          │    │
  │  │                                                                        │    │
  │  │  Medium cluster (typical):                                             │    │
  │  │  - 10 executors × 8 GB × 4 cores = 80 GB total                         │    │
  │  │                                                                        │    │
  │  │  Large cluster (production):                                           │    │
  │  │  - 50 executors × 16 GB × 5 cores = 800 GB total                       │    │
  │  └────────────────────────────────────────────────────────────────────────┘    │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  TUNING FOR SPECIFIC WORKLOADS:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  HEAVY CACHING WORKLOAD (ML pipelines, iterative algorithms):                   │
  │  - Increase spark.memory.storageFraction to 0.6-0.7                             │
  │  - Use MEMORY_ONLY_SER for better efficiency                                    │
  │  - Consider OFF_HEAP storage for large datasets                                 │
  │                                                                                 │
  │  HEAVY SHUFFLE WORKLOAD (large joins, aggregations):                            │
  │  - Decrease spark.memory.storageFraction to 0.3-0.4                             │
  │  - Increase spark.sql.shuffle.partitions                                        │
  │  - Enable off-heap memory for Tungsten operations                               │
  │                                                                                 │
  │  STREAMING WORKLOAD:                                                            │
  │  - Lower spark.memory.fraction to leave room for micro-batches                  │
  │  - Keep storage fraction balanced                                               │
  │  - Tune spark.streaming.backpressure.enabled                                    │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Garbage Collection Tuning

GC can significantly impact Spark performance. Understanding GC is key to optimization.

```
═══════════════════════════════════════════════════════════════════════════════════════
                       GARBAGE COLLECTION TUNING
═══════════════════════════════════════════════════════════════════════════════════════

  JVM MEMORY GENERATIONS:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │                           JVM HEAP                                         │ │
  │  │  ┌───────────────────────────────┬─────────────────────────────────────┐  │ │
  │  │  │        YOUNG GENERATION       │         OLD GENERATION              │  │ │
  │  │  │  ┌───────┬───────┬─────────┐  │  ┌───────────────────────────────┐  │  │ │
  │  │  │  │ Eden  │  S0   │   S1    │  │  │                               │  │  │ │
  │  │  │  │       │       │         │  │  │   Long-lived objects          │  │  │ │
  │  │  │  │ New   │Survivor Spaces  │  │  │   (cached data)               │  │  │ │
  │  │  │  │objects│       │         │  │  │                               │  │  │ │
  │  │  │  └───────┴───────┴─────────┘  │  └───────────────────────────────┘  │  │ │
  │  │  │                               │                                     │  │ │
  │  │  │  Minor GC (fast, frequent)    │  Major GC (slow, infrequent)        │  │ │
  │  │  └───────────────────────────────┴─────────────────────────────────────┘  │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  GC COLLECTORS:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  PARALLEL GC (default for Java 8):                                              │
  │  + Good throughput                                                              │
  │  - Long pause times for large heaps                                             │
  │  - Use for batch jobs with smaller heaps                                        │
  │                                                                                 │
  │  G1 GC (recommended for Spark):                                                 │
  │  + Better pause times                                                           │
  │  + Works well with large heaps (>4GB)                                           │
  │  + Adaptive                                                                     │
  │  - Slightly lower throughput                                                    │
  │  - Use for: Large heaps, latency-sensitive workloads                            │
  │                                                                                 │
  │  ZGC / Shenandoah (Java 11+):                                                   │
  │  + Very low pause times (<10ms)                                                 │
  │  + Handles very large heaps                                                     │
  │  - Higher CPU overhead                                                          │
  │  - Use for: Streaming, low-latency requirements                                 │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

### GC Configuration Examples

```scala
// ============================================
// GC SETTINGS (set in spark-submit or spark-defaults.conf)
// ============================================

// G1 GC (recommended for most Spark workloads)
spark.conf.set("spark.executor.extraJavaOptions",
  "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M")

// Enable GC logging for debugging
spark.conf.set("spark.executor.extraJavaOptions",
  "-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")

// For large heaps (>32GB), increase region size
spark.conf.set("spark.executor.extraJavaOptions",
  "-XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:MaxGCPauseMillis=200")

// Or via spark-submit
// --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"
```

## Diagnosing Memory Issues

```
═══════════════════════════════════════════════════════════════════════════════════════
                     DIAGNOSING MEMORY ISSUES
═══════════════════════════════════════════════════════════════════════════════════════

  SYMPTOMS AND SOLUTIONS:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  SYMPTOM: OutOfMemoryError on executor                                          │
  │  ─────────────────────────────────────────                                      │
  │  Causes:                                                                        │
  │  • Executor heap too small                                                      │
  │  • Too many cores per executor                                                  │
  │  • Data skew causing one partition to be huge                                   │
  │  • UDF creating too many objects                                                │
  │                                                                                 │
  │  Solutions:                                                                     │
  │  • Increase spark.executor.memory                                               │
  │  • Reduce cores per executor                                                    │
  │  • Increase partitions: repartition() or spark.sql.shuffle.partitions           │
  │  • Fix data skew with salting                                                   │
  │  • Optimize UDFs                                                                │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  SYMPTOM: OutOfMemoryError on driver                                            │
  │  ───────────────────────────────────────                                        │
  │  Causes:                                                                        │
  │  • collect() on large dataset                                                   │
  │  • Large broadcast variable                                                     │
  │  • Too many small files causing metadata explosion                              │
  │                                                                                 │
  │  Solutions:                                                                     │
  │  • Increase spark.driver.memory                                                 │
  │  • Use take(n) instead of collect()                                             │
  │  • Write to file instead of collecting                                          │
  │  • Coalesce small files                                                         │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  SYMPTOM: Long GC pauses / GC overhead limit exceeded                           │
  │  ────────────────────────────────────────────────────                           │
  │  Causes:                                                                        │
  │  • Heap too small for workload                                                  │
  │  • Too many objects created                                                     │
  │  • Inefficient GC collector                                                     │
  │                                                                                 │
  │  Solutions:                                                                     │
  │  • Increase executor memory                                                     │
  │  • Use serialized storage (MEMORY_ONLY_SER)                                     │
  │  • Switch to G1 GC                                                              │
  │  • Reduce number of cores per executor                                          │
  │  • Use DataFrames instead of RDDs                                               │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  SYMPTOM: Container killed by YARN                                              │
  │  ─────────────────────────────────────                                          │
  │  Causes:                                                                        │
  │  • Off-heap memory not accounted for                                            │
  │  • Memory overhead too low                                                      │
  │  • Native library memory leaks                                                  │
  │                                                                                 │
  │  Solutions:                                                                     │
  │  • Increase spark.executor.memoryOverhead                                       │
  │  • Increase spark.yarn.executor.memoryOverhead                                  │
  │  • Account for off-heap in YARN settings                                        │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  SYMPTOM: Excessive disk spilling                                               │
  │  ────────────────────────────────────                                           │
  │  Causes:                                                                        │
  │  • Execution memory too small                                                   │
  │  • Too few partitions for data size                                             │
  │  • Large shuffle/sort operations                                                │
  │                                                                                 │
  │  Solutions:                                                                     │
  │  • Increase spark.memory.fraction                                               │
  │  • Decrease spark.memory.storageFraction                                        │
  │  • Increase spark.sql.shuffle.partitions                                        │
  │  • Use broadcast joins for small tables                                         │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Monitoring Memory in Spark UI

```
═══════════════════════════════════════════════════════════════════════════════════════
                     SPARK UI MEMORY METRICS
═══════════════════════════════════════════════════════════════════════════════════════

  EXECUTORS TAB:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Storage Memory:                                                                │
  │  ┌────────────────────────────────────────────────────────────────────────┐    │
  │  │  Used: 2.5 GB / 4 GB                                                   │    │
  │  │  ███████████████████████████░░░░░░░░░░░░░░                             │    │
  │  │                                                                        │    │
  │  │  Shows how much storage memory is used for caching                     │    │
  │  └────────────────────────────────────────────────────────────────────────┘    │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  STAGE DETAILS:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Task Metrics:                                                                  │
  │  ┌────────────────────────────────────────────────────────────────────────┐    │
  │  │  Shuffle Spill (Memory):  2.5 GB                                       │    │
  │  │  Shuffle Spill (Disk):    500 MB  ← WARNING: Disk spill!               │    │
  │  │                                                                        │    │
  │  │  If Disk > 0, you're spilling to disk (slow!)                          │    │
  │  └────────────────────────────────────────────────────────────────────────┘    │
  │                                                                                 │
  │  Peak Execution Memory:                                                         │
  │  ┌────────────────────────────────────────────────────────────────────────┐    │
  │  │  Shows maximum memory used during task execution                       │    │
  │  │  High values indicate memory pressure                                  │    │
  │  └────────────────────────────────────────────────────────────────────────┘    │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  ENVIRONMENT TAB:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Check actual configuration values:                                             │
  │  • spark.executor.memory                                                        │
  │  • spark.memory.fraction                                                        │
  │  • spark.memory.storageFraction                                                 │
  │  • spark.memory.offHeap.*                                                       │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Common Pitfalls and Solutions

### Pitfall 1: Collecting Large Datasets

```scala
// BAD: Brings all data to driver
val allData = hugeDF.collect()

// GOOD: Limit or sample
val sample = hugeDF.limit(1000).collect()
val sample = hugeDF.sample(0.01).collect()

// GOOD: Write to file instead
hugeDF.write.parquet("/output/path")
```

### Pitfall 2: Caching Everything

```scala
// BAD: Caching data only used once
val df = spark.read.parquet("/data")
df.cache()  // Unnecessary!
df.filter($"col" > 100).write.parquet("/output")

// GOOD: Only cache when reused
val df = spark.read.parquet("/data").cache()
val result1 = df.filter($"col" > 100)
val result2 = df.filter($"col" < 50)
// ... use both results multiple times
df.unpersist()
```

### Pitfall 3: Wrong Storage Level

```scala
// For memory-constrained environments
df.persist(StorageLevel.MEMORY_ONLY_SER)  // Uses less memory

// For critical data that must survive
df.persist(StorageLevel.MEMORY_AND_DISK)  // Spills to disk if needed

// For data larger than memory
df.persist(StorageLevel.DISK_ONLY)  // When caching is still valuable
```

### Pitfall 4: Ignoring Data Skew

```scala
// PROBLEM: One partition much larger than others
df.groupBy("skewed_key").count()

// SOLUTION: Increase partitions
spark.conf.set("spark.sql.shuffle.partitions", "1000")

// SOLUTION: Salt the key
df.withColumn("salted_key", concat($"skewed_key", lit("_"), (rand() * 10).cast("int")))
  .groupBy("salted_key").count()
  .withColumn("key", split($"salted_key", "_")(0))
  .groupBy("key").agg(sum("count"))
```

## Best Practices

### 1. Right-Size Your Executors

```scala
// Good starting point
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")

// Per-core memory: 8GB / 4 cores = 2GB per core (good ratio)
```

### 2. Monitor and Adjust

```scala
// Enable metrics for monitoring
spark.conf.set("spark.metrics.conf.*.sink.console.class",
  "org.apache.spark.metrics.sink.ConsoleSink")

// Check memory usage in code
val runtime = Runtime.getRuntime
println(s"Used memory: ${(runtime.totalMemory - runtime.freeMemory) / 1024 / 1024} MB")
```

### 3. Clean Up Resources

```scala
// Always unpersist when done
val cachedDF = df.cache()
// ... use cachedDF
cachedDF.unpersist()

// Destroy broadcast variables
val bc = spark.sparkContext.broadcast(data)
// ... use bc
bc.unpersist()
bc.destroy()
```

### 4. Use DataFrames Over RDDs

```scala
// RDDs: More memory overhead, less optimization
val rdd = sc.parallelize(data).map(...)

// DataFrames: Tungsten memory management, optimized
val df = data.toDF().select(...)
```

### 5. Configure for Your Cluster Manager

```scala
// YARN
spark.conf.set("spark.yarn.executor.memoryOverhead", "1g")

// Kubernetes
spark.conf.set("spark.kubernetes.memoryOverheadFactor", "0.1")

// Standalone
// Memory is simpler, just set executor.memory appropriately
```

## Instructions

1. **Read** this README thoroughly
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-13-memory-management && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~45 minutes

## Next
Continue to [kata-14-jobs-stages-tasks](../kata-14-jobs-stages-tasks/)
