# Kata 08: Caching and Persistence - Avoiding Recomputation

## Goal
Master RDD caching and persistence strategies, understand storage levels, learn when to cache, and manage memory effectively to optimize Spark application performance.

## Why Caching Matters

```
THE RECOMPUTATION PROBLEM
═══════════════════════════════════════════════════════════════════════════════

  Without caching, Spark recomputes the ENTIRE lineage for each action!

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  WITHOUT CACHING:                                                           │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  val rdd = sc.textFile("huge_file.txt")  // Source                         │
  │    .filter(expensiveFilter)               // Expensive                      │
  │    .map(complexTransformation)            // Expensive                      │
  │                                                                             │
  │  rdd.count()    // Reads file → filter → map → count                       │
  │  rdd.take(10)   // Reads file AGAIN → filter AGAIN → map AGAIN → take     │
  │  rdd.reduce()   // Reads file AGAIN → filter AGAIN → map AGAIN → reduce   │
  │                                                                             │
  │  Total work: 3× file reads, 3× filters, 3× maps                            │
  │                                                                             │
  │                                                                             │
  │  WITH CACHING:                                                              │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  val rdd = sc.textFile("huge_file.txt")                                    │
  │    .filter(expensiveFilter)                                                 │
  │    .map(complexTransformation)                                              │
  │    .cache()   // ← Mark for caching                                        │
  │                                                                             │
  │  rdd.count()    // Reads file → filter → map → CACHE → count               │
  │  rdd.take(10)   // Uses CACHE → take (instant!)                            │
  │  rdd.reduce()   // Uses CACHE → reduce (instant!)                          │
  │                                                                             │
  │  Total work: 1× file read, 1× filter, 1× map, then cache hits             │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## cache() vs persist()

```
UNDERSTANDING CACHE AND PERSIST
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  cache() = persist(StorageLevel.MEMORY_ONLY)                               │
  │                                                                             │
  │  They're the same! cache() is just a shorthand.                            │
  │                                                                             │
  │  // These are equivalent:                                                   │
  │  rdd.cache()                                                                │
  │  rdd.persist()                                                              │
  │  rdd.persist(StorageLevel.MEMORY_ONLY)                                     │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  IMPORTANT: Caching is LAZY!
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  val rdd = sc.parallelize(1 to 1000000).map(heavyComputation)              │
  │                                                                             │
  │  rdd.cache()  // Just marks RDD - NOTHING is cached yet!                   │
  │                                                                             │
  │  rdd.count()  // First action: computes AND caches                         │
  │  rdd.take(5)  // Second action: uses cache                                 │
  │                                                                             │
  │  Timeline:                                                                  │
  │                                                                             │
  │  cache() called        count() called        take(5) called                │
  │       │                     │                      │                        │
  │       ▼                     ▼                      ▼                        │
  │  [Mark for cache]    [Compute + Cache]      [Read from cache]              │
  │  (instant)           (slow - real work)     (fast!)                        │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Storage Levels Deep Dive

```
STORAGE LEVEL OPTIONS
═══════════════════════════════════════════════════════════════════════════════

┌────────────────────────┬────────┬──────┬────────────┬────────────┬──────────┐
│ Storage Level          │ Memory │ Disk │ Serialized │ Replicated │ Off-Heap │
├────────────────────────┼────────┼──────┼────────────┼────────────┼──────────┤
│ MEMORY_ONLY (default)  │   ✓    │      │            │            │          │
├────────────────────────┼────────┼──────┼────────────┼────────────┼──────────┤
│ MEMORY_AND_DISK        │   ✓    │  ✓   │            │            │          │
├────────────────────────┼────────┼──────┼────────────┼────────────┼──────────┤
│ MEMORY_ONLY_SER        │   ✓    │      │     ✓      │            │          │
├────────────────────────┼────────┼──────┼────────────┼────────────┼──────────┤
│ MEMORY_AND_DISK_SER    │   ✓    │  ✓   │     ✓      │            │          │
├────────────────────────┼────────┼──────┼────────────┼────────────┼──────────┤
│ DISK_ONLY              │        │  ✓   │     ✓      │            │          │
├────────────────────────┼────────┼──────┼────────────┼────────────┼──────────┤
│ MEMORY_ONLY_2          │   ✓    │      │            │     2x     │          │
├────────────────────────┼────────┼──────┼────────────┼────────────┼──────────┤
│ MEMORY_AND_DISK_2      │   ✓    │  ✓   │            │     2x     │          │
├────────────────────────┼────────┼──────┼────────────┼────────────┼──────────┤
│ OFF_HEAP               │        │      │     ✓      │            │    ✓     │
└────────────────────────┴────────┴──────┴────────────┴────────────┴──────────┘


STORAGE LEVEL DECISION TREE:
─────────────────────────────────────────────────────────────────────────────

                        ┌─────────────────────┐
                        │ Does RDD fit in     │
                        │ memory comfortably? │
                        └──────────┬──────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
                   YES                            NO
                    │                             │
                    ▼                             ▼
         ┌─────────────────────┐     ┌─────────────────────┐
         │ Is deserialization  │     │ Is recomputation    │
         │ cost acceptable?    │     │ cheaper than disk?  │
         └──────────┬──────────┘     └──────────┬──────────┘
                    │                           │
             ┌──────┴──────┐             ┌──────┴──────┐
             │             │             │             │
            YES           NO            YES           NO
             │             │             │             │
             ▼             ▼             ▼             ▼
       ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌────────────────┐
       │ MEMORY_  │  │ MEMORY_   │  │ MEMORY_  │  │ MEMORY_AND_    │
       │ ONLY     │  │ ONLY_SER  │  │ ONLY     │  │ DISK or        │
       │          │  │           │  │ (let it  │  │ MEMORY_AND_    │
       └──────────┘  └───────────┘  │ recompute)│  │ DISK_SER      │
                                    └──────────┘  └────────────────┘
```

## Storage Level Details

```
DETAILED COMPARISON OF STORAGE LEVELS
═══════════════════════════════════════════════════════════════════════════════


  MEMORY_ONLY (default for cache())
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  How it works:                                                              │
  │  • Stores RDD partitions as deserialized Java objects in JVM heap          │
  │  • If partition doesn't fit, it's NOT stored (recomputed on access)        │
  │                                                                             │
  │  Pros:                                    Cons:                             │
  │  ✓ Fastest access (no deserialization)   ✗ High memory usage               │
  │  ✓ Direct object access                  ✗ GC pressure from many objects   │
  │                                          ✗ Lost partitions recomputed      │
  │                                                                             │
  │  Best for:                                                                  │
  │  • RDDs that fit comfortably in memory                                     │
  │  • When fast access is critical                                            │
  │  • Simple data types                                                        │
  │                                                                             │
  │  Memory usage: ~2-5x the raw data size (object overhead)                   │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  MEMORY_AND_DISK
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  How it works:                                                              │
  │  • First tries to store in memory as deserialized objects                  │
  │  • If memory is insufficient, spills partitions to disk                    │
  │                                                                             │
  │  ┌────────────────────┐    ┌────────────────────┐                          │
  │  │      Memory        │    │        Disk        │                          │
  │  │  [P0] [P1] [P2]    │    │  [P3] [P4]         │                          │
  │  │  (fits in memory)  │    │  (spilled to disk) │                          │
  │  └────────────────────┘    └────────────────────┘                          │
  │                                                                             │
  │  Pros:                                    Cons:                             │
  │  ✓ Won't lose partitions                 ✗ Disk access is slower           │
  │  ✓ Memory partitions still fast          ✗ I/O overhead for spilled parts  │
  │                                                                             │
  │  Best for:                                                                  │
  │  • RDDs larger than available memory                                       │
  │  • When you can't afford to recompute                                      │
  │  • Interactive workloads                                                    │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  MEMORY_ONLY_SER
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  How it works:                                                              │
  │  • Stores RDD partitions as serialized byte arrays                         │
  │  • One byte array per partition                                            │
  │  • Must deserialize on each access                                         │
  │                                                                             │
  │  Memory comparison (1 million Person objects):                              │
  │                                                                             │
  │  MEMORY_ONLY:                     MEMORY_ONLY_SER (with Kryo):             │
  │  ┌────────────────────────┐      ┌────────────────────────┐                │
  │  │ 1M separate objects    │      │ 1 byte array           │                │
  │  │ ~500 MB                │      │ ~100 MB                │                │
  │  │ + object headers       │      │ (5x smaller!)          │                │
  │  │ + references           │      │                        │                │
  │  └────────────────────────┘      └────────────────────────┘                │
  │                                                                             │
  │  Pros:                                    Cons:                             │
  │  ✓ 2-5x more space efficient             ✗ CPU cost for de/serialization  │
  │  ✓ Less GC pressure (fewer objects)      ✗ Slower access                   │
  │  ✓ Can fit more data in same memory                                        │
  │                                                                             │
  │  Best for:                                                                  │
  │  • Memory-constrained environments                                         │
  │  • Large objects with lots of fields                                       │
  │  • When using Kryo serializer                                              │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  DISK_ONLY
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  How it works:                                                              │
  │  • Stores all partitions on disk only                                      │
  │  • No memory usage for cached data                                         │
  │  • Must read from disk on each access                                      │
  │                                                                             │
  │  Pros:                                    Cons:                             │
  │  ✓ Handles any size RDD                  ✗ Slowest access                  │
  │  ✓ No memory pressure                    ✗ Disk I/O on every access        │
  │                                                                             │
  │  Best for:                                                                  │
  │  • Very expensive recomputation                                            │
  │  • RDDs way larger than memory                                             │
  │  • Batch processing (not interactive)                                      │
  │                                                                             │
  │  Rarely used - usually MEMORY_AND_DISK_SER is better                       │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  _2 VARIANTS (Replicated storage)
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  MEMORY_ONLY_2, MEMORY_AND_DISK_2                                          │
  │                                                                             │
  │  Each partition stored on TWO nodes:                                        │
  │                                                                             │
  │  Node 1          Node 2          Node 3                                    │
  │  ┌─────────┐    ┌─────────┐    ┌─────────┐                                 │
  │  │ [P0]    │    │ [P0]    │    │         │  P0 on nodes 1 & 2             │
  │  │ [P1]    │    │         │    │ [P1]    │  P1 on nodes 1 & 3             │
  │  └─────────┘    └─────────┘    └─────────┘                                 │
  │                                                                             │
  │  Benefits:                                                                  │
  │  ✓ Faster recovery if a node dies (use replica, don't recompute)          │
  │  ✓ Higher availability for critical data                                   │
  │                                                                             │
  │  Cost:                                                                      │
  │  ✗ 2x memory/disk usage                                                    │
  │  ✗ More network traffic during caching                                     │
  │                                                                             │
  │  Best for:                                                                  │
  │  • Long-running applications                                                │
  │  • Very expensive lineage (long chain of transformations)                  │
  │  • Fault-critical applications                                              │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## When to Cache

```
CACHING DECISION GUIDE
═══════════════════════════════════════════════════════════════════════════════


  CACHE WHEN:
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  1. RDD IS USED MULTIPLE TIMES                                              │
  │     ─────────────────────────                                               │
  │                                                                             │
  │     val processed = rdd.map(f).filter(g)                                   │
  │     processed.cache()                                                       │
  │                                                                             │
  │     // Multiple downstream uses                                             │
  │     val count = processed.count()                                           │
  │     val sample = processed.take(100)                                        │
  │     val output = processed.saveAsTextFile("...")                           │
  │                                                                             │
  │                                                                             │
  │  2. RDD IS AT A BRANCH POINT                                                │
  │     ────────────────────────                                                │
  │                                                                             │
  │                      ┌────► analysis1                                       │
  │     source → process ┼────► analysis2                                       │
  │                      └────► analysis3                                       │
  │                  ↑                                                          │
  │            CACHE HERE!                                                      │
  │                                                                             │
  │                                                                             │
  │  3. RDD IS EXPENSIVE TO COMPUTE                                             │
  │     ─────────────────────────                                               │
  │                                                                             │
  │     val expensive = rdd                                                     │
  │       .map(cpuIntensiveFunction)                                           │
  │       .filter(complexCondition)                                             │
  │       .flatMap(networkCall)                                                 │
  │                                                                             │
  │     expensive.cache()  // Worth caching even for 2 uses                    │
  │                                                                             │
  │                                                                             │
  │  4. ITERATIVE ALGORITHMS                                                    │
  │     ──────────────────                                                      │
  │                                                                             │
  │     // Machine learning, graph algorithms, etc.                            │
  │     var data = initialData.cache()                                          │
  │     for (i <- 1 to iterations) {                                           │
  │       val newData = data.map(update)                                        │
  │       newData.cache()                                                       │
  │       data.unpersist()  // Free previous iteration                         │
  │       data = newData                                                        │
  │     }                                                                       │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  DON'T CACHE WHEN:
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  1. RDD IS USED ONLY ONCE                                                   │
  │     ────────────────────                                                    │
  │                                                                             │
  │     // No benefit from caching                                              │
  │     val result = rdd.map(f).filter(g).reduce(combine)                      │
  │     // Done - never accessed again                                          │
  │                                                                             │
  │                                                                             │
  │  2. RDD IS CHEAP TO RECOMPUTE                                               │
  │     ──────────────────────                                                  │
  │                                                                             │
  │     val simple = rdd.filter(_ > 0)  // Very fast                           │
  │     // May be faster to recompute than manage cache                        │
  │                                                                             │
  │                                                                             │
  │  3. RDD IS TOO LARGE FOR MEMORY (without disk fallback)                    │
  │     ────────────────────────────────────────────────────                    │
  │                                                                             │
  │     val huge = sc.textFile("petabyte_file.txt")                            │
  │     huge.cache()  // Will evict other cached data!                         │
  │     // Use MEMORY_AND_DISK_SER instead, or don't cache                     │
  │                                                                             │
  │                                                                             │
  │  4. DATA CHANGES BETWEEN ACCESSES (streaming)                               │
  │     ────────────────────────────────────────────                            │
  │                                                                             │
  │     // In streaming, each batch is new data                                │
  │     // Caching RDDs from old batches wastes memory                         │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Code Examples

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Basic caching
// ═══════════════════════════════════════════════════════════════════════════

import org.apache.spark.storage.StorageLevel

val rdd = sc.textFile("data.txt")
  .map(parseLine)
  .filter(isValid)

// Option 1: cache() - shorthand for MEMORY_ONLY
rdd.cache()

// Option 2: persist() with specific storage level
rdd.persist(StorageLevel.MEMORY_AND_DISK)

// Trigger caching with first action
val count = rdd.count()

// Subsequent actions use cache
val first10 = rdd.take(10)  // Fast!
val sample = rdd.sample(false, 0.1).collect()  // Fast!


// ═══════════════════════════════════════════════════════════════════════════
// Choosing storage levels
// ═══════════════════════════════════════════════════════════════════════════

// Memory only (default, fastest access)
rdd.persist(StorageLevel.MEMORY_ONLY)

// Memory with disk spillover (safer)
rdd.persist(StorageLevel.MEMORY_AND_DISK)

// Serialized (more space efficient)
rdd.persist(StorageLevel.MEMORY_ONLY_SER)

// Serialized with disk spillover (good balance)
rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

// Disk only (for very large RDDs)
rdd.persist(StorageLevel.DISK_ONLY)

// Replicated (for fault tolerance)
rdd.persist(StorageLevel.MEMORY_AND_DISK_2)


// ═══════════════════════════════════════════════════════════════════════════
// Unpersisting to free memory
// ═══════════════════════════════════════════════════════════════════════════

val intermediate = rdd.map(transform1)
intermediate.cache()

// Use intermediate
val result1 = intermediate.filter(c1).count()
val result2 = intermediate.filter(c2).count()

// Done with intermediate - free memory!
intermediate.unpersist()

// Continue with other processing
val final = rdd.map(transform2).filter(condition).count()


// ═══════════════════════════════════════════════════════════════════════════
// Checking cache status
// ═══════════════════════════════════════════════════════════════════════════

rdd.cache()
rdd.count()  // Triggers caching

// Check if RDD is cached
println(rdd.getStorageLevel)  // StorageLevel(memory, deserialized, 1 replicas)

// Check via Spark UI or programmatically
val cachedPartitions = rdd.getNumPartitions
println(s"RDD has $cachedPartitions partitions")

// In Spark UI: Storage tab shows cached RDDs


// ═══════════════════════════════════════════════════════════════════════════
// Iterative algorithm pattern
// ═══════════════════════════════════════════════════════════════════════════

var data = initialData.map(prepare).cache()
data.count()  // Materialize cache

for (iteration <- 1 to maxIterations) {
  // Compute new data based on current
  val newData = data.flatMap(expand).reduceByKey(combine)

  // Cache the new data
  newData.cache()
  newData.count()  // Materialize

  // Unpersist old data to free memory
  data.unpersist()

  // Update reference
  data = newData

  println(s"Iteration $iteration complete")
}

val finalResult = data.collect()


// ═══════════════════════════════════════════════════════════════════════════
// DataFrame/Dataset caching
// ═══════════════════════════════════════════════════════════════════════════

// DataFrames have the same caching methods
val df = spark.read.parquet("data.parquet")
  .filter($"status" === "active")
  .cache()

df.count()  // Materialize cache

// Multiple queries on cached data
df.groupBy("region").count().show()
df.groupBy("product").agg(sum("revenue")).show()

df.unpersist()
```

## Memory Management

```
UNDERSTANDING SPARK MEMORY
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  EXECUTOR MEMORY LAYOUT (Unified Memory Management)                         │
  │                                                                             │
  │  ┌─────────────────────────────────────────────────────────────────────────┐│
  │  │                      Total Executor Memory                              ││
  │  │                                                                         ││
  │  │  ┌───────────────────────────────────────────────────────────────────┐ ││
  │  │  │            Reserved Memory (300MB fixed)                          │ ││
  │  │  │            For Spark internal objects                             │ ││
  │  │  └───────────────────────────────────────────────────────────────────┘ ││
  │  │                                                                         ││
  │  │  ┌───────────────────────────────────────────────────────────────────┐ ││
  │  │  │                   User Memory (40% of remaining)                  │ ││
  │  │  │                   For user data structures                        │ ││
  │  │  └───────────────────────────────────────────────────────────────────┘ ││
  │  │                                                                         ││
  │  │  ┌───────────────────────────────────────────────────────────────────┐ ││
  │  │  │             Unified Memory (60% of remaining)                     │ ││
  │  │  │  ┌─────────────────────┬─────────────────────┐                    │ ││
  │  │  │  │   Storage Memory    │  Execution Memory   │                    │ ││
  │  │  │  │   (for caching)     │  (for shuffles,     │                    │ ││
  │  │  │  │                     │   sorts, joins)     │                    │ ││
  │  │  │  │                     │                     │                    │ ││
  │  │  │  │  ◄──── Can borrow from each other ────►  │                    │ ││
  │  │  │  │                     │                     │                    │ ││
  │  │  │  └─────────────────────┴─────────────────────┘                    │ ││
  │  │  └───────────────────────────────────────────────────────────────────┘ ││
  │  │                                                                         ││
  │  └─────────────────────────────────────────────────────────────────────────┘│
  │                                                                             │
  │  Default: spark.memory.fraction = 0.6 (60% for unified memory)             │
  │           spark.memory.storageFraction = 0.5 (50% of unified for storage)  │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  WHAT HAPPENS WHEN MEMORY IS FULL:
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  SCENARIO 1: Cache fills up (MEMORY_ONLY)                                   │
  │                                                                             │
  │  Storage Memory:  [P0][P1][P2][P3][FULL]                                   │
  │                                                                             │
  │  New partition P4 arrives...                                                │
  │  → LRU eviction: Remove least recently used partition                      │
  │  → P0 evicted, P4 stored                                                   │
  │                                                                             │
  │  Storage Memory:  [P4][P1][P2][P3]                                         │
  │                                                                             │
  │  If P0 is accessed again: RECOMPUTED from lineage                          │
  │                                                                             │
  │                                                                             │
  │  SCENARIO 2: Cache fills up (MEMORY_AND_DISK)                               │
  │                                                                             │
  │  Storage Memory:  [P0][P1][P2][P3][FULL]                                   │
  │                                                                             │
  │  New partition P4 arrives...                                                │
  │  → LRU partition P0 spilled to disk                                        │
  │  → P4 stored in memory                                                     │
  │                                                                             │
  │  Storage Memory:  [P4][P1][P2][P3]                                         │
  │  Disk:            [P0]                                                      │
  │                                                                             │
  │  If P0 is accessed: Read from disk (slow but no recompute)                 │
  │                                                                             │
  │                                                                             │
  │  SCENARIO 3: Execution needs memory                                         │
  │                                                                             │
  │  Storage: [P0][P1][P2]   Execution: [FULL - needs more for join]           │
  │                                                                             │
  │  → Execution can borrow from Storage                                       │
  │  → Cached partitions evicted to make room                                  │
  │  → After execution, storage can reclaim space                              │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Monitoring and Debugging

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Check what's cached
// ═══════════════════════════════════════════════════════════════════════════

// Check storage level
println(rdd.getStorageLevel)
// StorageLevel(memory, deserialized, 1 replicas)

// Check if actually cached (non-empty after action)
println(s"Is cached: ${rdd.getStorageLevel.useMemory || rdd.getStorageLevel.useDisk}")


// ═══════════════════════════════════════════════════════════════════════════
// Spark UI: Storage Tab
// ═══════════════════════════════════════════════════════════════════════════

// Navigate to http://localhost:4040/storage/
//
// Shows for each cached RDD:
// - Storage Level
// - Size in Memory
// - Size on Disk
// - Number of Partitions (Cached / Total)
// - Fraction Cached


// ═══════════════════════════════════════════════════════════════════════════
// Programmatic cache inspection
// ═══════════════════════════════════════════════════════════════════════════

// Get SparkContext's storage status
val storageStatus = sc.getExecutorStorageStatus

storageStatus.foreach { status =>
  println(s"Executor: ${status.blockManagerId}")
  println(s"  Memory used: ${status.memUsed} bytes")
  println(s"  Memory remaining: ${status.memRemaining} bytes")
  println(s"  Disk used: ${status.diskUsed} bytes")
}


// ═══════════════════════════════════════════════════════════════════════════
// Debugging cache issues
// ═══════════════════════════════════════════════════════════════════════════

// Problem: Cache not being used
// Check 1: Did you call an action to materialize?
rdd.cache()
// rdd.count()  // ← Don't forget this!

// Check 2: Is storage level set?
println(rdd.getStorageLevel)  // Should not be NONE

// Check 3: Is there enough memory?
// Look at Spark UI Storage tab - check "Fraction Cached"


// Problem: Cache being evicted
// Solution: Use MEMORY_AND_DISK to prevent data loss
rdd.persist(StorageLevel.MEMORY_AND_DISK)

// Or increase storage memory
// spark.memory.storageFraction = 0.6  (increase from default 0.5)
```

## Common Pitfalls

```
PITFALL 1: Caching without an action
─────────────────────────────────────────────────────────────────────────────

  // WRONG: Cache is never materialized
  val rdd = sc.textFile("data.txt").map(f)
  rdd.cache()
  // ... later ...
  val count = rdd.count()  // Still not cached until here!

  // RIGHT: Materialize immediately if you want it cached now
  rdd.cache()
  rdd.count()  // Forces caching


PITFALL 2: Forgetting to unpersist
─────────────────────────────────────────────────────────────────────────────

  // Memory leak!
  for (file <- files) {
    val rdd = sc.textFile(file).cache()
    rdd.count()
    // Process...
    // Forgot to unpersist! Memory fills up.
  }

  // CORRECT:
  for (file <- files) {
    val rdd = sc.textFile(file).cache()
    rdd.count()
    // Process...
    rdd.unpersist()  // Free memory
  }


PITFALL 3: Caching too early in the pipeline
─────────────────────────────────────────────────────────────────────────────

  // BAD: Caching before filter (more data than needed)
  val rdd = sc.textFile("huge.txt")
    .cache()  // Caches everything!
    .filter(condition)  // Then filters

  // GOOD: Cache after filtering
  val rdd = sc.textFile("huge.txt")
    .filter(condition)  // Filter first
    .cache()  // Cache smaller dataset


PITFALL 4: Wrong storage level for use case
─────────────────────────────────────────────────────────────────────────────

  // BAD: MEMORY_ONLY for data larger than memory
  hugeRdd.persist(StorageLevel.MEMORY_ONLY)
  // Results in constant eviction and recomputation

  // GOOD: Use MEMORY_AND_DISK_SER
  hugeRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)


PITFALL 5: Caching intermediate RDDs in long lineage
─────────────────────────────────────────────────────────────────────────────

  // May run out of memory caching all intermediate RDDs
  val step1 = rdd.map(f1).cache()
  val step2 = step1.map(f2).cache()
  val step3 = step2.map(f3).cache()
  val step4 = step3.map(f4).cache()

  // BETTER: Only cache what you'll reuse
  val step3 = rdd.map(f1).map(f2).map(f3).cache()  // Branch point
  val result1 = step3.map(a).collect()
  val result2 = step3.map(b).collect()
```

## Best Practices

```
BEST PRACTICE 1: Cache at branch points
─────────────────────────────────────────────────────────────────────────────

  val processed = raw.map(f).filter(g).cache()

  // Multiple uses from same cached RDD
  val analysis1 = processed.map(a1).reduce(...)
  val analysis2 = processed.map(a2).reduce(...)
  val analysis3 = processed.map(a3).reduce(...)


BEST PRACTICE 2: Use appropriate storage level
─────────────────────────────────────────────────────────────────────────────

  // Default for small RDDs that fit in memory
  smallRdd.cache()

  // For larger RDDs or uncertain memory
  mediumRdd.persist(StorageLevel.MEMORY_AND_DISK)

  // For memory-constrained environments
  largeRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)


BEST PRACTICE 3: Always unpersist when done
─────────────────────────────────────────────────────────────────────────────

  val cached = rdd.map(expensive).cache()
  cached.count()

  // Do all work with cached RDD
  val result1 = cached.filter(c1).collect()
  val result2 = cached.filter(c2).collect()

  // Clean up
  cached.unpersist()


BEST PRACTICE 4: Monitor cache usage
─────────────────────────────────────────────────────────────────────────────

  // Check Spark UI Storage tab regularly
  // Watch for:
  // - Fraction Cached < 100% (memory pressure)
  // - RDDs you forgot to unpersist
  // - Unexpected storage levels


BEST PRACTICE 5: Consider Kryo serialization
─────────────────────────────────────────────────────────────────────────────

  // Kryo is faster and more compact than Java serialization
  val conf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[MyClass]))

  // Then MEMORY_ONLY_SER is more efficient
  rdd.persist(StorageLevel.MEMORY_ONLY_SER)
```

## Instructions

1. **Read** this README thoroughly - understand caching mechanics
2. **Open** `src/main/scala/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-08-caching-persistence && sbt run`
4. **Monitor**: Check Spark UI Storage tab to see caching in action
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **cache() = persist(MEMORY_ONLY)** - they're the same
2. **Caching is lazy** - needs an action to materialize
3. **Cache when RDD is reused** - multiple actions or branch points
4. **MEMORY_ONLY** - fastest, for RDDs that fit in memory
5. **MEMORY_AND_DISK** - safer, spills to disk when full
6. **MEMORY_ONLY_SER** - more space efficient, slower access
7. **Always unpersist** when done to free memory
8. **Monitor via Spark UI** - Storage tab shows cached RDDs
9. **LRU eviction** - least recently used partitions removed first
10. **Cache after filters** - store smaller dataset

## Time
~35 minutes

## Next
Continue to [kata-09-dataframes](../kata-09-dataframes/)
