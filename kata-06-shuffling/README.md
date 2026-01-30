# Kata 06: Shuffling - Understanding Spark's Most Expensive Operation

## Goal
Master the shuffle mechanism in Spark, understand shuffle read/write operations, learn which operations cause shuffles, and discover techniques to minimize or avoid shuffles entirely.

## What is a Shuffle?

```
SHUFFLE = REDISTRIBUTION OF DATA ACROSS PARTITIONS
═══════════════════════════════════════════════════════════════════════════════

  A shuffle occurs when data needs to be reorganized so that records with
  the same key end up in the same partition.

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  BEFORE SHUFFLE (groupByKey on "color"):                                    │
  │                                                                             │
  │  Partition 0           Partition 1           Partition 2                    │
  │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
  │  │ (red, 1)     │     │ (blue, 4)    │     │ (red, 7)     │                │
  │  │ (blue, 2)    │     │ (red, 5)     │     │ (green, 8)   │                │
  │  │ (green, 3)   │     │ (green, 6)   │     │ (blue, 9)    │                │
  │  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
  │         │                    │                    │                         │
  │         └────────────────────┼────────────────────┘                         │
  │                              │                                              │
  │                       ┌──────┴──────┐                                       │
  │                       │   SHUFFLE   │  ← Network I/O                       │
  │                       │             │    Disk I/O                          │
  │                       │  (EXPENSIVE)│    Serialization                     │
  │                       └──────┬──────┘                                       │
  │                              │                                              │
  │         ┌────────────────────┼────────────────────┐                         │
  │         │                    │                    │                         │
  │         ▼                    ▼                    ▼                         │
  │  Partition 0           Partition 1           Partition 2                    │
  │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
  │  │ (red, 1)     │     │ (blue, 2)    │     │ (green, 3)   │                │
  │  │ (red, 5)     │     │ (blue, 4)    │     │ (green, 6)   │                │
  │  │ (red, 7)     │     │ (blue, 9)    │     │ (green, 8)   │                │
  │  └──────────────┘     └──────────────┘     └──────────────┘                │
  │      All reds            All blues           All greens                    │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Shuffle Mechanics Deep Dive

```
THE SHUFFLE PROCESS - MAP AND REDUCE SIDES
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  ══════════════════════════════════════════════════════════════════════    │
  │  STAGE N (Map Side / Shuffle Write)                                        │
  │  ══════════════════════════════════════════════════════════════════════    │
  │                                                                             │
  │  Executor 1                      Executor 2                                 │
  │  ┌────────────────────┐         ┌────────────────────┐                     │
  │  │ Task 0: Partition 0│         │ Task 1: Partition 1│                     │
  │  │                    │         │                    │                     │
  │  │  1. Transform data │         │  1. Transform data │                     │
  │  │  2. Compute target │         │  2. Compute target │                     │
  │  │     partition      │         │     partition      │                     │
  │  │  3. Sort by key    │         │  3. Sort by key    │                     │
  │  │  4. Write shuffle  │         │  4. Write shuffle  │                     │
  │  │     files to disk  │         │     files to disk  │                     │
  │  │                    │         │                    │                     │
  │  │  Shuffle Files:    │         │  Shuffle Files:    │                     │
  │  │  ┌─────┐┌─────┐   │         │  ┌─────┐┌─────┐   │                     │
  │  │  │ P0  ││ P1  │   │         │  │ P0  ││ P1  │   │                     │
  │  │  │data ││data │   │         │  │data ││data │   │                     │
  │  │  └─────┘└─────┘   │         │  └─────┘└─────┘   │                     │
  │  └────────────────────┘         └────────────────────┘                     │
  │           │                              │                                  │
  │           └──────────────┬───────────────┘                                  │
  │                          │                                                  │
  │  ════════════════════════│══════════════════════════════════════════════   │
  │                          │  SHUFFLE (Network Transfer)                     │
  │  ════════════════════════│══════════════════════════════════════════════   │
  │                          │                                                  │
  │           ┌──────────────┴───────────────┐                                  │
  │           │                              │                                  │
  │           ▼                              ▼                                  │
  │  ══════════════════════════════════════════════════════════════════════    │
  │  STAGE N+1 (Reduce Side / Shuffle Read)                                    │
  │  ══════════════════════════════════════════════════════════════════════    │
  │                                                                             │
  │  Executor 1                      Executor 2                                 │
  │  ┌────────────────────┐         ┌────────────────────┐                     │
  │  │ Task 0: Partition 0│         │ Task 1: Partition 1│                     │
  │  │                    │         │                    │                     │
  │  │  1. Fetch shuffle  │         │  1. Fetch shuffle  │                     │
  │  │     blocks from    │         │     blocks from    │                     │
  │  │     ALL executors  │         │     ALL executors  │                     │
  │  │  2. Merge & sort   │         │  2. Merge & sort   │                     │
  │  │  3. Process data   │         │  3. Process data   │                     │
  │  │                    │         │                    │                     │
  │  └────────────────────┘         └────────────────────┘                     │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  SHUFFLE FILE STRUCTURE:
  ─────────────────────────────────────────────────────────────────────────────

  Each map task creates one file per reduce partition:

  Map Task 0 →  shuffle_0_0.data  (for reduce partition 0)
                shuffle_0_1.data  (for reduce partition 1)
                shuffle_0_2.data  (for reduce partition 2)
                shuffle_0.index   (offsets into data files)

  Map Task 1 →  shuffle_1_0.data
                shuffle_1_1.data
                shuffle_1_2.data
                shuffle_1.index

  Total files = M (map tasks) × R (reduce partitions) + M (index files)
```

## The Cost of Shuffling

```
WHY SHUFFLES ARE EXPENSIVE
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  1. DISK I/O (Shuffle Write)                                                │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • Map side writes sorted data to local disk                               │
  │  • Even with SSD, disk is 10-100x slower than memory                       │
  │  • Large shuffles can exhaust disk space                                   │
  │                                                                             │
  │  2. NETWORK I/O (Shuffle Transfer)                                          │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • Data moves across network between executors                             │
  │  • Network is 10-1000x slower than memory                                  │
  │  • Can saturate network bandwidth                                          │
  │  • Cross-rack traffic is even slower                                       │
  │                                                                             │
  │  3. DISK I/O (Shuffle Read)                                                 │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • Reduce side reads and merges shuffle files                              │
  │  • External merge sort for large data                                      │
  │  • Spill to disk if memory insufficient                                    │
  │                                                                             │
  │  4. SERIALIZATION/DESERIALIZATION                                           │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • Objects must be serialized for disk/network                             │
  │  • And deserialized on the other side                                      │
  │  • CPU intensive, especially for complex objects                           │
  │                                                                             │
  │  5. GC PRESSURE                                                             │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • Many temporary objects created during shuffle                           │
  │  • Can trigger long GC pauses                                              │
  │  • Impacts overall job latency                                             │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  PERFORMANCE COMPARISON:
  ─────────────────────────────────────────────────────────────────────────────

  Operation Type          Typical Latency       Relative Speed
  ─────────────────────────────────────────────────────────────
  Memory access           ~100 nanoseconds      1x (baseline)
  SSD read                ~100 microseconds     1,000x slower
  HDD read                ~10 milliseconds      100,000x slower
  Network (same rack)     ~0.5 milliseconds     5,000x slower
  Network (cross-DC)      ~50 milliseconds      500,000x slower

  SHUFFLE = All of the slow things combined!
```

## Operations That Cause Shuffles

```
SHUFFLE-INDUCING OPERATIONS
═══════════════════════════════════════════════════════════════════════════════

┌────────────────────┬────────────┬───────────────────────────────────────────┐
│ Operation          │ Shuffle?   │ Why / Notes                               │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ groupByKey         │ YES        │ Groups ALL values by key                  │
│                    │            │ AVOID if possible!                        │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ reduceByKey        │ YES*       │ Shuffle, but combines locally first       │
│                    │            │ Much better than groupByKey!              │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ aggregateByKey     │ YES*       │ Like reduceByKey with different types     │
│                    │            │ Combines locally first                    │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ combineByKey       │ YES*       │ Most general aggregation                  │
│                    │            │ Combines locally first                    │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ join               │ YES**      │ Brings matching keys together             │
│                    │            │ **Unless co-partitioned!                  │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ cogroup            │ YES**      │ Groups matching keys from multiple RDDs   │
│                    │            │ **Unless co-partitioned!                  │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ distinct           │ YES        │ Must compare all elements                 │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ sortByKey          │ YES        │ Requires range partitioning               │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ repartition        │ YES        │ Redistributes data                        │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ coalesce           │ NO***      │ Merges partitions locally                 │
│                    │            │ ***Unless shuffle=true                    │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ map, filter        │ NO         │ Per-element, no data movement             │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ flatMap            │ NO         │ Per-element transformation                │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ mapPartitions      │ NO         │ Per-partition, no data movement           │
├────────────────────┼────────────┼───────────────────────────────────────────┤
│ union              │ NO         │ Just concatenates partition lists         │
└────────────────────┴────────────┴───────────────────────────────────────────┘
```

## reduceByKey vs groupByKey - The Critical Difference

```
THE MOST IMPORTANT OPTIMIZATION: reduceByKey over groupByKey
═══════════════════════════════════════════════════════════════════════════════

  GOAL: Sum values by key for data like: (a,1), (a,2), (a,3), (b,4), (b,5)


  groupByKey APPROACH:
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Partition 0                Partition 1                                     │
  │  (a, 1)                     (a, 3)                                          │
  │  (a, 2)                     (b, 4)                                          │
  │  (b, 5)                                                                     │
  │     │                          │                                            │
  │     │    ALL VALUES SHIPPED    │                                            │
  │     │    ACROSS NETWORK!       │                                            │
  │     ▼                          ▼                                            │
  │  ═══════════════════ SHUFFLE ═══════════════════                           │
  │     │                          │                                            │
  │     ▼                          ▼                                            │
  │  (a, [1, 2, 3]) ──sum──► (a, 6)                                            │
  │  (b, [4, 5])    ──sum──► (b, 9)                                            │
  │                                                                             │
  │  Network transfer: 5 values                                                 │
  │  Memory on reduce side: Must hold ALL values per key                       │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  reduceByKey APPROACH:
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Partition 0                Partition 1                                     │
  │  (a, 1)                     (a, 3)                                          │
  │  (a, 2) ──LOCAL──► (a, 3)   (b, 4)                                          │
  │  (b, 5)   COMBINE           │                                               │
  │     │                       │                                               │
  │     │  ONLY PARTIAL SUMS    │                                               │
  │     │  SHIPPED!             │                                               │
  │     ▼                       ▼                                               │
  │  ═══════════════════ SHUFFLE ═══════════════════                           │
  │     │                       │                                               │
  │     ▼                       ▼                                               │
  │  (a, 3) + (a, 3) = (a, 6)                                                  │
  │  (b, 5) + (b, 4) = (b, 9)                                                  │
  │                                                                             │
  │  Network transfer: 4 partial sums (less than 5!)                           │
  │  Memory on reduce side: Only one value per key at a time                   │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  DRAMATIC DIFFERENCE AT SCALE:
  ─────────────────────────────────────────────────────────────────────────────

  1 million records with 1000 unique keys:

  groupByKey:   Ships 1,000,000 values across network
                Reduce tasks must hold ~1000 values per key in memory

  reduceByKey:  Ships ~1,000 partial sums (1000x less data!)
                Reduce tasks only need space for one value per key
```

```scala
// ═══════════════════════════════════════════════════════════════════════════
// ALWAYS prefer reduceByKey over groupByKey when aggregating
// ═══════════════════════════════════════════════════════════════════════════

val wordPairs = words.map(w => (w, 1))

// BAD - sends all values across network
val badWordCounts = wordPairs
  .groupByKey()           // Shuffle: all values
  .mapValues(_.sum)       // Then sum locally

// GOOD - combines locally before shuffle
val goodWordCounts = wordPairs
  .reduceByKey(_ + _)     // Local combine + shuffle partial sums


// ═══════════════════════════════════════════════════════════════════════════
// Use aggregateByKey for different input/output types
// ═══════════════════════════════════════════════════════════════════════════

// Calculate (sum, count) to compute average later
val sumCount = pairs.aggregateByKey((0.0, 0))(
  // seqOp: combine value into accumulator (within partition)
  (acc, value) => (acc._1 + value, acc._2 + 1),
  // combOp: merge accumulators (across partitions)
  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)

val averages = sumCount.mapValues { case (sum, count) => sum / count }


// ═══════════════════════════════════════════════════════════════════════════
// combineByKey - Most general (reduceByKey and aggregateByKey use this)
// ═══════════════════════════════════════════════════════════════════════════

val result = pairs.combineByKey(
  // createCombiner: first value for a key
  (v: Int) => (v, 1),
  // mergeValue: add value to accumulator
  (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1),
  // mergeCombiners: merge two accumulators
  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)
```

## Avoiding Shuffles Entirely

```
SHUFFLE AVOIDANCE STRATEGIES
═══════════════════════════════════════════════════════════════════════════════


  STRATEGY 1: BROADCAST JOINS
  ─────────────────────────────────────────────────────────────────────────────

  When one side of a join is small enough to fit in memory:

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  SHUFFLE JOIN (Both sides shuffled):                                        │
  │                                                                             │
  │  Large RDD                       Small RDD                                  │
  │  ┌──────────┐                   ┌──────────┐                               │
  │  │ 100 GB   │                   │  100 MB  │                               │
  │  └────┬─────┘                   └────┬─────┘                               │
  │       │                              │                                      │
  │       └──────────────┬───────────────┘                                      │
  │                      │                                                      │
  │               ┌──────┴──────┐                                               │
  │               │   SHUFFLE   │  Both sides shuffled!                        │
  │               │   (SLOW)    │  100GB + 100MB moved                         │
  │               └─────────────┘                                               │
  │                                                                             │
  │                                                                             │
  │  BROADCAST JOIN (Small side broadcast to all executors):                    │
  │                                                                             │
  │  Large RDD                       Small RDD                                  │
  │  ┌──────────┐                   ┌──────────┐                               │
  │  │ 100 GB   │                   │  100 MB  │                               │
  │  │ (stays)  │                   └────┬─────┘                               │
  │  └────┬─────┘                        │                                      │
  │       │                     BROADCAST (100MB to each executor)             │
  │       │                              │                                      │
  │       │     ┌────────────────────────┼────────────────────────┐            │
  │       ▼     ▼                        ▼                        ▼            │
  │    ┌────────────┐             ┌────────────┐            ┌────────────┐     │
  │    │ Executor 1 │             │ Executor 2 │            │ Executor 3 │     │
  │    │ Local join │             │ Local join │            │ Local join │     │
  │    │ with       │             │ with       │            │ with       │     │
  │    │ broadcast  │             │ broadcast  │            │ broadcast  │     │
  │    └────────────┘             └────────────┘            └────────────┘     │
  │                                                                             │
  │    NO SHUFFLE of large RDD!                                                │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Broadcast join implementation
// ═══════════════════════════════════════════════════════════════════════════

// Small lookup table
val lookupTable = sc.parallelize(Seq(
  ("US", "United States"),
  ("UK", "United Kingdom"),
  ("CA", "Canada")
))

// Large fact table
val transactions = sc.parallelize(/* millions of records */)

// Option 1: Shuffle join (expensive!)
val joined = transactions.join(lookupTable)

// Option 2: Broadcast join (no shuffle of transactions!)
val lookupMap = lookupTable.collectAsMap()
val broadcastLookup = sc.broadcast(lookupMap)

val broadcastJoined = transactions.mapPartitions { iter =>
  val localLookup = broadcastLookup.value
  iter.map { case (countryCode, amount) =>
    (countryCode, (amount, localLookup.getOrElse(countryCode, "Unknown")))
  }
}


// ═══════════════════════════════════════════════════════════════════════════
// DataFrame broadcast join hint
// ═══════════════════════════════════════════════════════════════════════════

import org.apache.spark.sql.functions.broadcast

val result = largeDF.join(broadcast(smallDF), "key")
```

```
  STRATEGY 2: CO-PARTITIONING
  ─────────────────────────────────────────────────────────────────────────────

  Pre-partition both RDDs with same partitioner for shuffle-free joins:

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  // First time: Both RDDs need to be partitioned                           │
  │  val users = userRdd.partitionBy(new HashPartitioner(100)).cache()         │
  │  val orders = orderRdd.partitionBy(new HashPartitioner(100))               │
  │                                                                             │
  │  users.partitioner == orders.partitioner  // true!                         │
  │                                                                             │
  │  // Join is now shuffle-free - each partition joined locally               │
  │  val joined = users.join(orders)  // NO SHUFFLE!                           │
  │                                                                             │
  │                                                                             │
  │  Partition 0 (users)    +    Partition 0 (orders)    →    Partition 0     │
  │  [user1, user5]              [order1, order3]              [joined]        │
  │                                                                             │
  │  Partition 1 (users)    +    Partition 1 (orders)    →    Partition 1     │
  │  [user2, user3]              [order2, order4]              [joined]        │
  │                                                                             │
  │  Each partition joined locally - no network transfer!                      │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Co-partitioning for multiple joins
// ═══════════════════════════════════════════════════════════════════════════

import org.apache.spark.HashPartitioner

// Partition all RDDs the same way
val partitioner = new HashPartitioner(100)

val users = userRdd.partitionBy(partitioner).cache()
val orders = orderRdd.partitionBy(partitioner)
val clicks = clickRdd.partitionBy(partitioner)

// All subsequent joins are shuffle-free!
val userOrders = users.join(orders)     // No shuffle
val userClicks = users.join(clicks)     // No shuffle
val orderClicks = orders.join(clicks)   // No shuffle


// ═══════════════════════════════════════════════════════════════════════════
// Check if RDDs are co-partitioned
// ═══════════════════════════════════════════════════════════════════════════

def isCoPartitioned(rdd1: RDD[_], rdd2: RDD[_]): Boolean = {
  (rdd1.partitioner, rdd2.partitioner) match {
    case (Some(p1), Some(p2)) => p1 == p2
    case _ => false
  }
}

println(isCoPartitioned(users, orders))  // true
```

```
  STRATEGY 3: MAP-SIDE OPERATIONS
  ─────────────────────────────────────────────────────────────────────────────

  Do as much work as possible before any shuffle:

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  BAD: Filter after shuffle                                                  │
  │                                                                             │
  │  data ──► groupByKey() ──► filter() ──► result                             │
  │           (shuffles ALL)   (filters few)                                   │
  │                                                                             │
  │  Shuffled 1M records, then filtered to 10K                                 │
  │                                                                             │
  │                                                                             │
  │  GOOD: Filter before shuffle                                                │
  │                                                                             │
  │  data ──► filter() ──► groupByKey() ──► result                             │
  │           (filters early)  (shuffles less)                                 │
  │                                                                             │
  │  Filtered to 10K records, then shuffled only 10K                           │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Push filters before shuffles
// ═══════════════════════════════════════════════════════════════════════════

// BAD: Filter after expensive shuffle
val bad = data
  .groupByKey()
  .filter { case (k, v) => v.size > 10 }

// GOOD: Filter as early as possible
val good = data
  .filter { case (k, v) => isRelevant(k) }  // Filter before groupBy
  .groupByKey()


// ═══════════════════════════════════════════════════════════════════════════
// Combine transformations before shuffle
// ═══════════════════════════════════════════════════════════════════════════

// BAD: Multiple shuffles
val step1 = data.reduceByKey(_ + _)  // Shuffle 1
val step2 = step1.mapValues(_ * 2)
val step3 = step2.reduceByKey(_ + _)  // Shuffle 2 (but only if different partitioning)

// GOOD: Single shuffle
val result = data
  .mapValues(_ * 2)
  .reduceByKey(_ + _)  // Single shuffle
```

## Monitoring Shuffles

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Check for shuffles in execution plan
// ═══════════════════════════════════════════════════════════════════════════

val result = data.reduceByKey(_ + _)

println(result.toDebugString)
// Look for "ShuffledRDD" or "+-" (wide dependency indicator)

// (8) ShuffledRDD[5] at reduceByKey at <console>:25
//  +-(8) MapPartitionsRDD[4] at map at <console>:24
//     |  ParallelCollectionRDD[0]

// The +- indicates a shuffle boundary (new stage)


// ═══════════════════════════════════════════════════════════════════════════
// Count shuffles in a query
// ═══════════════════════════════════════════════════════════════════════════

def countShuffles(rdd: RDD[_]): Int = {
  rdd.dependencies.map {
    case _: ShuffleDependency[_, _, _] => 1 + countShuffles(rdd.dependencies.head.rdd)
    case dep => countShuffles(dep.rdd)
  }.sum
}


// ═══════════════════════════════════════════════════════════════════════════
// Spark UI metrics to monitor
// ═══════════════════════════════════════════════════════════════════════════

// In Spark UI (http://localhost:4040), check:
//
// 1. Stages tab:
//    - Shuffle Read: Data read from shuffle files
//    - Shuffle Write: Data written to shuffle files
//    - Shuffle Spill (Memory): Spilled to memory during shuffle
//    - Shuffle Spill (Disk): Spilled to disk during shuffle
//
// 2. Jobs tab:
//    - Number of stages (each shuffle = new stage)
//
// 3. Executors tab:
//    - Shuffle Read/Write per executor

// Key metrics to watch:
// - Shuffle read/write size (minimize these!)
// - Shuffle spill (indicates memory pressure)
// - Stage boundaries (fewer is better)
```

## Common Pitfalls

```
PITFALL 1: Using groupByKey for aggregation
─────────────────────────────────────────────────────────────────────────────

  // NEVER do this!
  val sums = pairs.groupByKey().mapValues(_.sum)

  // ALWAYS do this!
  val sums = pairs.reduceByKey(_ + _)


PITFALL 2: Unnecessary repartition
─────────────────────────────────────────────────────────────────────────────

  // BAD: Repartition before reduce (double shuffle!)
  val result = data.repartition(100).reduceByKey(_ + _)

  // GOOD: Let reduceByKey set partitions
  val result = data.reduceByKey(_ + _, 100)


PITFALL 3: Not leveraging existing partitioning
─────────────────────────────────────────────────────────────────────────────

  // After partitionBy, use mapValues to preserve partitioner
  val partitioned = pairs.partitionBy(new HashPartitioner(100))

  // BAD: map loses partitioner, next operation shuffles
  val bad = partitioned.map { case (k, v) => (k, v + 1) }

  // GOOD: mapValues preserves partitioner
  val good = partitioned.mapValues(_ + 1)


PITFALL 4: Shuffling before filter
─────────────────────────────────────────────────────────────────────────────

  // BAD: Shuffle everything, then filter
  val result = pairs.groupByKey().filter(_._2.size > 10)

  // GOOD: Try to filter before shuffle
  // (Though not always possible with groupByKey)
  val result = pairs.filter(needsProcessing).groupByKey()


PITFALL 5: Not caching before multiple shuffles
─────────────────────────────────────────────────────────────────────────────

  val expensive = data.map(heavyTransform)

  // BAD: heavyTransform runs twice!
  val agg1 = expensive.reduceByKey(_ + _)
  val agg2 = expensive.groupByKey()

  // GOOD: Cache and reuse
  expensive.cache()
  val agg1 = expensive.reduceByKey(_ + _)
  val agg2 = expensive.groupByKey()
```

## Best Practices

```
BEST PRACTICE 1: Profile before optimizing
─────────────────────────────────────────────────────────────────────────────

  // Use Spark UI to find shuffle bottlenecks
  // Focus on stages with highest shuffle read/write


BEST PRACTICE 2: Use appropriate aggregation operations
─────────────────────────────────────────────────────────────────────────────

  // From best to worst:
  // 1. reduceByKey/aggregateByKey (local combine first)
  // 2. combineByKey (full control)
  // 3. groupByKey (AVOID - shuffles all values)


BEST PRACTICE 3: Broadcast small tables
─────────────────────────────────────────────────────────────────────────────

  // If one side < 10MB (configurable), use broadcast join
  val broadcastThreshold = 10 * 1024 * 1024  // 10MB
  if (smallRdd.count() * avgRecordSize < broadcastThreshold) {
    // Use broadcast join
  }


BEST PRACTICE 4: Co-partition related RDDs
─────────────────────────────────────────────────────────────────────────────

  // Partition once, join many times without shuffle
  val partitioner = new HashPartitioner(200)
  val users = userRdd.partitionBy(partitioner).cache()
  val orders = orderRdd.partitionBy(partitioner).cache()


BEST PRACTICE 5: Configure shuffle parameters
─────────────────────────────────────────────────────────────────────────────

  // spark.sql.shuffle.partitions = 200 (default, tune for your data)
  // spark.shuffle.compress = true (compress shuffle data)
  // spark.shuffle.spill.compress = true (compress spill data)
  // spark.reducer.maxSizeInFlight = 48m (shuffle fetch buffer)
```

## Instructions

1. **Read** this README thoroughly - understand shuffle mechanics
2. **Open** `src/main/scala/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-06-shuffling && sbt run`
4. **Monitor**: Use toDebugString to verify shuffle reduction
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **Shuffles are expensive** - disk I/O + network I/O + serialization
2. **reduceByKey over groupByKey** - combines locally before shuffle
3. **Broadcast joins** - eliminate shuffle for small tables
4. **Co-partitioning** - pre-partition for shuffle-free joins
5. **Filter early** - reduce data before shuffle
6. **mapValues preserves partitioner** - map does not
7. **Monitor shuffle metrics** - use Spark UI
8. **Stage boundaries indicate shuffles** - check toDebugString
9. **Cache before multiple shuffles** - avoid recomputation
10. **Tune shuffle partitions** - spark.sql.shuffle.partitions

## Time
~40 minutes

## Next
Continue to [kata-07-narrow-wide-transformations](../kata-07-narrow-wide-transformations/)
