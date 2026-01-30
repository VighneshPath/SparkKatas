# Kata 05: Partitioning - The Key to Parallelism

## Goal
Master data partitioning in Spark, understand Hash and Range partitioners, learn when to use repartition vs coalesce, and implement custom partitioning strategies.

## Why Partitioning Matters

```
PARTITIONING = THE FOUNDATION OF PARALLELISM
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │   PARTITIONS = UNITS OF PARALLELISM                                         │
  │                                                                             │
  │   RDD with 4 partitions = 4 tasks = 4 parallel computations                │
  │                                                                             │
  │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
  │   │  Partition 0 │  │  Partition 1 │  │  Partition 2 │  │  Partition 3 │   │
  │   │   [1-250]    │  │  [251-500]   │  │  [501-750]   │  │ [751-1000]   │   │
  │   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
  │          │                 │                 │                 │            │
  │          ▼                 ▼                 ▼                 ▼            │
  │       Task 0            Task 1            Task 2            Task 3          │
  │      (Executor 1)      (Executor 2)      (Executor 1)      (Executor 3)    │
  │                                                                             │
  │   KEY INSIGHTS:                                                             │
  │   • More partitions → More parallelism (up to available cores)             │
  │   • Each partition processed by exactly ONE task                            │
  │   • Partition = Scheduling + Memory + I/O unit                             │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## The Goldilocks Problem: Partition Count

```
FINDING THE RIGHT PARTITION COUNT
═══════════════════════════════════════════════════════════════════════════════

  TOO FEW PARTITIONS                    TOO MANY PARTITIONS
  ──────────────────                    ────────────────────

  ┌────────────────────────┐            ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐
  │                        │            │ ││ ││ ││ ││ ││ ││ ││ │
  │    HUGE PARTITION      │            └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘
  │        (10GB)          │            ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐
  │                        │            │ ││ ││ ││ ││ ││ ││ ││ │
  └────────────────────────┘            └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘
                                        (1000 tiny partitions)

  PROBLEMS:                             PROBLEMS:
  • Underutilizes CPU cores             • Excessive scheduling overhead
  • Memory pressure (OOM risk)          • Too many small files on write
  • Long GC pauses                      • High task launch latency
  • One slow task blocks job            • Network overhead for coordination
  • Data skew amplified                 • Driver memory pressure (metadata)


  GUIDELINES FOR PARTITION SIZING
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  RULE OF THUMB:                                                             │
  │                                                                             │
  │  • 2-4 partitions per CPU core in the cluster                              │
  │  • Each partition should be 100MB - 1GB (ideally ~200MB)                   │
  │  • Total partitions = cluster_cores * 2 to cluster_cores * 4               │
  │                                                                             │
  │  EXAMPLE:                                                                   │
  │  • 100 core cluster                                                         │
  │  • 50GB dataset                                                             │
  │  • Ideal: 200-400 partitions (128-256MB each)                              │
  │                                                                             │
  │  FORMULA:                                                                   │
  │  partitions = max(num_cores * 2, data_size_MB / 200)                       │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Inspecting Partitions

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Get partition count
// ═══════════════════════════════════════════════════════════════════════════

val rdd = sc.parallelize(1 to 1000, 8)

println(rdd.getNumPartitions)  // 8
println(rdd.partitions.length) // 8 (same thing)


// ═══════════════════════════════════════════════════════════════════════════
// glom() - View partition contents
// ═══════════════════════════════════════════════════════════════════════════

// glom() collects each partition into an array
val partitionContents = rdd.glom().collect()

partitionContents.zipWithIndex.foreach { case (elements, partitionId) =>
  println(s"Partition $partitionId: ${elements.length} elements")
  println(s"  First: ${elements.headOption}, Last: ${elements.lastOption}")
}

// Output might be:
// Partition 0: 125 elements (1-125)
// Partition 1: 125 elements (126-250)
// ...


// ═══════════════════════════════════════════════════════════════════════════
// mapPartitionsWithIndex - Process with partition info
// ═══════════════════════════════════════════════════════════════════════════

val partitionInfo = rdd.mapPartitionsWithIndex { (idx, iter) =>
  val elements = iter.toList
  Iterator(s"Partition $idx has ${elements.size} elements: ${elements.take(3)}...")
}

partitionInfo.collect().foreach(println)


// ═══════════════════════════════════════════════════════════════════════════
// Check partitioner
// ═══════════════════════════════════════════════════════════════════════════

val pairs = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))

println(pairs.partitioner)  // None - no partitioner assigned

val partitioned = pairs.partitionBy(new HashPartitioner(4))
println(partitioned.partitioner)  // Some(HashPartitioner(4))
```

## Hash Partitioner

```
HASH PARTITIONER - DEFAULT FOR KEY-VALUE OPERATIONS
═══════════════════════════════════════════════════════════════════════════════

  The Hash Partitioner determines partition using:

  partition = hash(key) % numPartitions

  (Actually: partition = (key.hashCode % numPartitions + numPartitions) % numPartitions
   to handle negative hash codes)


  EXAMPLE WITH 4 PARTITIONS:
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Input records:                                                             │
  │  ("apple", 1), ("banana", 2), ("cherry", 3), ("date", 4),                  │
  │  ("apple", 5), ("elderberry", 6), ("fig", 7), ("grape", 8)                 │
  │                                                                             │
  │  Hash values (simplified):                                                  │
  │  "apple".hashCode % 4 = 1    → Partition 1                                 │
  │  "banana".hashCode % 4 = 2   → Partition 2                                 │
  │  "cherry".hashCode % 4 = 0   → Partition 0                                 │
  │  "date".hashCode % 4 = 3     → Partition 3                                 │
  │  "elderberry".hashCode % 4 = 1 → Partition 1                               │
  │  "fig".hashCode % 4 = 2      → Partition 2                                 │
  │  "grape".hashCode % 4 = 3    → Partition 3                                 │
  │                                                                             │
  │  Result:                                                                    │
  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌────────────┐ │
  │  │  Partition 0   │  │  Partition 1   │  │  Partition 2   │  │ Partition 3│ │
  │  │  (cherry, 3)   │  │  (apple, 1)    │  │  (banana, 2)   │  │ (date, 4)  │ │
  │  │                │  │  (apple, 5)    │  │  (fig, 7)      │  │ (grape, 8) │ │
  │  │                │  │  (elderberry,6)│  │                │  │            │ │
  │  └────────────────┘  └────────────────┘  └────────────────┘  └────────────┘ │
  │                                                                             │
  │  KEY PROPERTY: Same key ALWAYS goes to same partition                       │
  │  This is crucial for efficient joins and aggregations!                      │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

```scala
// Using HashPartitioner
import org.apache.spark.HashPartitioner

val pairs = sc.parallelize(Seq(
  ("apple", 1), ("banana", 2), ("apple", 3), ("cherry", 4)
))

// Apply hash partitioning with 3 partitions
val hashPartitioned = pairs.partitionBy(new HashPartitioner(3))

// Operations that preserve partitioner
val summed = hashPartitioned.reduceByKey(_ + _)
println(summed.partitioner)  // Some(HashPartitioner(3)) - preserved!

// Operations that create HashPartitioner automatically
val grouped = pairs.groupByKey(4)  // Creates HashPartitioner(4)
val reduced = pairs.reduceByKey(_ + _, 4)  // Creates HashPartitioner(4)
```

## Range Partitioner

```
RANGE PARTITIONER - FOR SORTED DATA AND RANGE QUERIES
═══════════════════════════════════════════════════════════════════════════════

  The Range Partitioner divides the key space into roughly equal ranges:

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Sorted keys: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]                   │
  │                                                                             │
  │  With 4 partitions, Range Partitioner finds boundaries:                     │
  │                                                                             │
  │    keys < 15       15 <= keys < 30    30 <= keys < 45    keys >= 45        │
  │  ┌─────────────┐  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    │
  │  │ Partition 0 │  │ Partition 1 │    │ Partition 2 │    │ Partition 3 │    │
  │  │  1, 5, 10   │  │  15, 20, 25 │    │  30, 35, 40 │    │   45, 50    │    │
  │  └─────────────┘  └─────────────┘    └─────────────┘    └─────────────┘    │
  │                                                                             │
  │  KEY PROPERTIES:                                                            │
  │  • Ranges determined by sampling the data                                   │
  │  • Partitions are approximately equal in size                               │
  │  • Data within each partition is NOT sorted (only boundaries matter)       │
  │  • Used by sortByKey()                                                      │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  WHEN TO USE RANGE PARTITIONER:
  ─────────────────────────────────────────────────────────────────────────────

  • Sorted output needed: sortByKey(), sortBy()
  • Range queries: Find all records where key between A and B
  • Uniform distribution with ordered keys
  • Time-series data partitioned by timestamp ranges

  HASH vs RANGE:
  ─────────────────────────────────────────────────────────────────────────────

  Hash Partitioner:
  • O(1) lookup for specific key
  • Random distribution
  • Good for joins on specific keys
  • Used by: groupByKey, reduceByKey, join

  Range Partitioner:
  • Efficient range scans
  • Ordered distribution
  • Good for range queries
  • Used by: sortByKey, repartitionAndSortWithinPartitions
```

```scala
import org.apache.spark.RangePartitioner

val pairs = sc.parallelize((1 to 100).map(i => (i, s"value-$i")))

// Create RangePartitioner (samples the data)
val rangePartitioner = new RangePartitioner(4, pairs)

val rangePartitioned = pairs.partitionBy(rangePartitioner)

// Check partition boundaries
rangePartitioned.mapPartitionsWithIndex { (idx, iter) =>
  val keys = iter.map(_._1).toList
  Iterator(s"Partition $idx: keys ${keys.min} to ${keys.max}")
}.collect().foreach(println)

// sortByKey automatically uses RangePartitioner
val sorted = pairs.sortByKey()
println(sorted.partitioner)  // Some(RangePartitioner(...))
```

## Custom Partitioners

```
CUSTOM PARTITIONER - DOMAIN-SPECIFIC PARTITIONING
═══════════════════════════════════════════════════════════════════════════════

  Sometimes Hash and Range partitioners don't fit your needs:

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  USE CASE: Partition by first letter of customer name                       │
  │                                                                             │
  │  Data: (Alice, ...), (Bob, ...), (Amy, ...), (Brian, ...), (Carl, ...)     │
  │                                                                             │
  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
  │  │  Partition 0   │  │  Partition 1   │  │  Partition 2   │                │
  │  │  Names A-H     │  │  Names I-P     │  │  Names Q-Z     │                │
  │  │  Alice, Amy,   │  │  Jake, Mary,   │  │  Quinn, Steve, │                │
  │  │  Bob, Brian,   │  │  Oscar, Paul   │  │  Tom, Zack     │                │
  │  │  Carl...       │  │                │  │                │                │
  │  └────────────────┘  └────────────────┘  └────────────────┘                │
  │                                                                             │
  │  USE CASE: Partition by geographic region                                   │
  │                                                                             │
  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
  │  │  Partition 0   │  │  Partition 1   │  │  Partition 2   │                │
  │  │  East Coast    │  │  West Coast    │  │  Midwest       │                │
  │  │  NY, MA, FL    │  │  CA, WA, OR    │  │  IL, OH, TX    │                │
  │  └────────────────┘  └────────────────┘  └────────────────┘                │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

```scala
import org.apache.spark.Partitioner

// Custom partitioner by first letter
class AlphabetPartitioner(val numPartitions: Int) extends Partitioner {

  require(numPartitions > 0, "Number of partitions must be positive")

  def getPartition(key: Any): Int = {
    val firstChar = key.toString.toUpperCase.head
    if (firstChar >= 'A' && firstChar <= 'Z') {
      // Distribute A-Z across partitions
      ((firstChar - 'A') * numPartitions / 26) % numPartitions
    } else {
      // Non-alphabetic keys go to partition 0
      0
    }
  }

  // IMPORTANT: equals and hashCode must be overridden!
  override def equals(other: Any): Boolean = other match {
    case p: AlphabetPartitioner => p.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode(): Int = numPartitions
}

// Usage
val names = sc.parallelize(Seq(
  ("Alice", 100), ("Bob", 200), ("Charlie", 150),
  ("Amy", 120), ("Brian", 180), ("Zack", 90)
))

val partitioned = names.partitionBy(new AlphabetPartitioner(3))


// Custom partitioner for geographic regions
class RegionPartitioner extends Partitioner {

  val regions = Map(
    "NY" -> 0, "MA" -> 0, "FL" -> 0, "CT" -> 0,  // East
    "CA" -> 1, "WA" -> 1, "OR" -> 1, "NV" -> 1,  // West
    "IL" -> 2, "OH" -> 2, "TX" -> 2, "MI" -> 2   // Central
  )

  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    regions.getOrElse(key.toString.toUpperCase, 0)
  }

  override def equals(other: Any): Boolean = other.isInstanceOf[RegionPartitioner]
  override def hashCode(): Int = 42
}
```

## repartition vs coalesce

```
REPARTITION VS COALESCE - CHANGING PARTITION COUNT
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  repartition(n)                        coalesce(n)                         │
  │  ──────────────                        ───────────                         │
  │                                                                             │
  │  • ALWAYS shuffles                     • NO shuffle (when reducing)        │
  │  • Can increase or decrease            • Best for reducing partitions      │
  │  • Uniform distribution                • May have uneven partitions        │
  │  • More expensive                      • Cheaper (no data movement)        │
  │  • Full network I/O                    • Local merging                     │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  VISUALIZATION:
  ─────────────────────────────────────────────────────────────────────────────

  repartition(3) - Shuffle redistributes data evenly:

  BEFORE (4 partitions)               AFTER (3 partitions)
  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐    ┌─────────┐ ┌─────────┐ ┌─────────┐
  │ 100 │ │ 200 │ │  50 │ │ 150 │    │   167   │ │   167   │ │   166   │
  │ el  │ │ el  │ │ el  │ │ el  │    │   el    │ │   el    │ │   el    │
  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘    └─────────┘ └─────────┘ └─────────┘
     │       │       │       │              ▲          ▲          ▲
     └───────┴───────┴───────┴──── SHUFFLE ─┴──────────┴──────────┘


  coalesce(3) - Merge adjacent partitions (no shuffle):

  BEFORE (4 partitions)               AFTER (3 partitions)
  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐    ┌─────────┐ ┌─────┐ ┌─────────┐
  │ 100 │ │ 200 │ │  50 │ │ 150 │    │   300   │ │  50 │ │   150   │
  │ el  │ │ el  │ │ el  │ │ el  │    │   el    │ │ el  │ │   el    │
  └──┬──┘ └──┬──┘ └─────┘ └─────┘    └─────────┘ └─────┘ └─────────┘
     │       │                             ▲
     └───────┴────── LOCAL MERGE ──────────┘

     Notice: Partitions can be uneven!


  WHEN TO USE WHICH:
  ─────────────────────────────────────────────────────────────────────────────

  Use repartition():
  • Increasing partition count
  • Need uniform partition sizes
  • Data is heavily skewed
  • After filtering that removes most data

  Use coalesce():
  • Decreasing partition count
  • Just before writing output files
  • When shuffle cost isn't worth uniform sizes
  • Reducing many small partitions to fewer larger ones
```

```scala
val rdd = sc.parallelize(1 to 1000, 100)  // 100 partitions

// Reduce partitions - prefer coalesce (no shuffle)
val fewer = rdd.coalesce(10)
println(fewer.getNumPartitions)  // 10

// Increase partitions - must use repartition
val more = rdd.repartition(200)
println(more.getNumPartitions)  // 200

// coalesce with shuffle = repartition (for increasing)
val moreWithCoalesce = rdd.coalesce(200, shuffle = true)
println(moreWithCoalesce.getNumPartitions)  // 200


// Common pattern: reduce before write
val processed = largeRdd
  .map(transform)
  .filter(condition)
  // After heavy filtering, many partitions may be nearly empty
  .coalesce(10)  // Merge to reasonable number before write

processed.saveAsTextFile("output/")


// Anti-pattern: coalesce then shuffle operation
val bad = rdd
  .coalesce(2)    // Reduce to 2 partitions
  .groupByKey()   // Shuffle! Now only 2-way parallelism

val better = rdd
  .groupByKey()   // Shuffle with full parallelism
  .coalesce(2)    // Then reduce
```

## Partition Preservation

```
OPERATIONS THAT PRESERVE PARTITIONING
═══════════════════════════════════════════════════════════════════════════════

  When you have a partitioned RDD, some operations preserve the partitioner,
  others lose it.

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  PRESERVE PARTITIONER:                                                      │
  │  ─────────────────────                                                      │
  │                                                                             │
  │  • mapValues      (key unchanged)                                           │
  │  • flatMapValues  (key unchanged)                                           │
  │  • filter         (no key changes)                                          │
  │  • reduceByKey    (same # partitions)                                       │
  │  • aggregateByKey (same # partitions)                                       │
  │  • join           (both sides same partitioner)                             │
  │                                                                             │
  │  LOSE PARTITIONER:                                                          │
  │  ─────────────────                                                          │
  │                                                                             │
  │  • map            (could change key!)                                       │
  │  • flatMap        (could change key!)                                       │
  │  • repartition    (new partitioning)                                        │
  │  • coalesce       (changes partition structure)                             │
  │  • distinct       (changes everything)                                      │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  WHY THIS MATTERS:
  ─────────────────────────────────────────────────────────────────────────────

  val partitioned = rdd.partitionBy(new HashPartitioner(100))

  // GOOD: Preserves partitioner
  val result1 = partitioned.mapValues(_ + 1)
  println(result1.partitioner)  // Some(HashPartitioner(100))

  // BAD: Loses partitioner
  val result2 = partitioned.map { case (k, v) => (k, v + 1) }
  println(result2.partitioner)  // None!

  // If you join result2 with another partitioned RDD, shuffle required!
```

```scala
// Pattern: Use mapValues instead of map when possible

val pairs = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
val partitioned = pairs.partitionBy(new HashPartitioner(3))

// BAD - loses partitioner
val bad = partitioned.map { case (k, v) => (k, v * 2) }
println(bad.partitioner)  // None

// GOOD - preserves partitioner
val good = partitioned.mapValues(_ * 2)
println(good.partitioner)  // Some(HashPartitioner(3))


// Checking before join
val rdd1 = pairs1.partitionBy(new HashPartitioner(10))
val rdd2 = pairs2.partitionBy(new HashPartitioner(10))

// If both have same partitioner, join is partition-local!
val joined = rdd1.join(rdd2)  // No shuffle needed!
```

## Data Skew and Partitioning

```
HANDLING DATA SKEW
═══════════════════════════════════════════════════════════════════════════════

  Data skew = Uneven data distribution across partitions

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  SKEWED DATA:                                                               │
  │                                                                             │
  │  ┌──────────────────────────────────────────┐                              │
  │  │                                          │                              │
  │  │              Partition 0                 │  90% of data!                │
  │  │           (Hot key: "USA")               │                              │
  │  │                                          │                              │
  │  └──────────────────────────────────────────┘                              │
  │  ┌─────┐ ┌─────┐ ┌─────┐                                                   │
  │  │ P1  │ │ P2  │ │ P3  │  10% of data split across these                  │
  │  └─────┘ └─────┘ └─────┘                                                   │
  │                                                                             │
  │  PROBLEM:                                                                   │
  │  • Partition 0 takes 10x longer than others                                │
  │  • Other executors sit idle waiting                                        │
  │  • Total job time = slowest partition time                                 │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  SOLUTIONS FOR DATA SKEW:
  ─────────────────────────────────────────────────────────────────────────────

  1. SALTING - Add random prefix to hot keys

     Original: ("USA", value)
     Salted:   ("USA_0", value), ("USA_1", value), ("USA_2", value)

     // Salt the keys
     val salted = rdd.map { case (key, value) =>
       val salt = scala.util.Random.nextInt(10)
       (s"${key}_$salt", value)
     }

     // Aggregate with salted keys (parallelized)
     val partialAgg = salted.reduceByKey(_ + _)

     // Remove salt and aggregate again
     val finalAgg = partialAgg.map { case (saltedKey, value) =>
       (saltedKey.split("_")(0), value)
     }.reduceByKey(_ + _)


  2. ISOLATE AND SPECIAL-CASE HOT KEYS

     val hotKeys = Set("USA", "China", "India")

     val (hot, normal) = rdd.partition(r => hotKeys.contains(r._1))

     // Process hot keys with more partitions
     val hotResult = hot.repartition(100).reduceByKey(_ + _)
     val normalResult = normal.reduceByKey(_ + _)

     hotResult.union(normalResult)


  3. BROADCAST SMALL SIDE IN JOINS

     // Instead of shuffle join:
     val joined = largeRdd.join(skewedRdd)  // Skew problem!

     // Broadcast the small side:
     val smallMap = smallRdd.collectAsMap()
     val broadcastMap = sc.broadcast(smallMap)

     val joined = largeRdd.mapPartitions { iter =>
       val localMap = broadcastMap.value
       iter.flatMap { case (k, v) =>
         localMap.get(k).map(v2 => (k, (v, v2)))
       }
     }
```

## Common Pitfalls

```
PITFALL 1: Shuffling when not necessary
─────────────────────────────────────────────────────────────────────────────

  // BAD: Two shuffles for same partitioning
  val agg1 = rdd.reduceByKey(_ + _)
  val agg2 = agg1.reduceByKey(_ + _)  // Shuffles again!

  // GOOD: Check if already partitioned
  val agg1 = rdd.reduceByKey(_ + _)
  println(agg1.partitioner)  // Some(HashPartitioner(n))
  // If same partitioning needed, no shuffle!


PITFALL 2: Losing partitioner unnecessarily
─────────────────────────────────────────────────────────────────────────────

  // BAD: map loses partitioner
  val partitioned = rdd.partitionBy(new HashPartitioner(10))
  val transformed = partitioned.map { case (k, v) => (k, v + 1) }
  // transformed.partitioner is None!

  // GOOD: mapValues preserves partitioner
  val transformed = partitioned.mapValues(_ + 1)
  // transformed.partitioner is Some(HashPartitioner(10))


PITFALL 3: Wrong partition count for output
─────────────────────────────────────────────────────────────────────────────

  // BAD: 10000 tiny files
  rdd.repartition(10000).saveAsTextFile("output/")

  // BAD: One huge file
  rdd.coalesce(1).saveAsTextFile("output/")

  // GOOD: Reasonable number based on data size
  val outputPartitions = Math.max(1, totalSizeMB / 128)
  rdd.coalesce(outputPartitions).saveAsTextFile("output/")


PITFALL 4: Ignoring data skew
─────────────────────────────────────────────────────────────────────────────

  // If one key has 90% of data, groupByKey will have one slow task
  val grouped = rdd.groupByKey()  // P0 takes 10x longer!

  // Solution: Salt hot keys or use reduceByKey
  val aggregated = rdd.reduceByKey(_ + _)  // Still skewed, but less data


PITFALL 5: repartition in a loop
─────────────────────────────────────────────────────────────────────────────

  // BAD: Shuffle on every iteration!
  var rdd = initialRdd
  for (i <- 1 to 10) {
    rdd = rdd.map(transform).repartition(100)  // Expensive!
  }

  // GOOD: Repartition once at the end if needed
  var rdd = initialRdd
  for (i <- 1 to 10) {
    rdd = rdd.map(transform)
  }
  rdd = rdd.repartition(100)
```

## Best Practices

```
BEST PRACTICE 1: Set partition count at source
─────────────────────────────────────────────────────────────────────────────

  // Set partitions when reading
  val rdd = sc.textFile("huge_file.txt", minPartitions = 100)

  // For parallelize
  val rdd = sc.parallelize(data, numSlices = numCores * 2)


BEST PRACTICE 2: Use coalesce before writing
─────────────────────────────────────────────────────────────────────────────

  // Avoid small files problem
  processedRdd
    .coalesce(targetPartitions)
    .saveAsTextFile("output/")

  // targetPartitions = ceil(dataSizeGB / 0.128)  // ~128MB files


BEST PRACTICE 3: Pre-partition for repeated joins
─────────────────────────────────────────────────────────────────────────────

  val users = userRdd.partitionBy(new HashPartitioner(100)).cache()
  val orders = orderRdd.partitionBy(new HashPartitioner(100))
  val clicks = clickRdd.partitionBy(new HashPartitioner(100))

  // All these joins are shuffle-free!
  val result1 = users.join(orders)
  val result2 = users.join(clicks)
  val result3 = orders.join(clicks)


BEST PRACTICE 4: Monitor partition sizes
─────────────────────────────────────────────────────────────────────────────

  // Check for skew
  val partitionSizes = rdd.mapPartitionsWithIndex { (idx, iter) =>
    Iterator((idx, iter.size))
  }.collect()

  val sizes = partitionSizes.map(_._2)
  println(s"Min: ${sizes.min}, Max: ${sizes.max}, Avg: ${sizes.sum/sizes.length}")

  // If max >> avg, you have skew!


BEST PRACTICE 5: Use appropriate partitioner for your access pattern
─────────────────────────────────────────────────────────────────────────────

  // Point lookups → HashPartitioner
  val userById = users.partitionBy(new HashPartitioner(100))

  // Range scans → RangePartitioner
  val timeSeriesData = events.partitionBy(new RangePartitioner(100, events))

  // Domain-specific → Custom Partitioner
  val byRegion = sales.partitionBy(new RegionPartitioner())
```

## Instructions

1. **Read** this README thoroughly - understand partitioning deeply
2. **Open** `src/main/scala/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-05-partitioning && sbt run`
4. **Experiment**: Use glom() to visualize partition contents
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **Partitions = units of parallelism** - one task per partition
2. **2-4 partitions per core** with 100MB-1GB each is optimal
3. **HashPartitioner** - for point lookups and joins
4. **RangePartitioner** - for sorted data and range queries
5. **Custom Partitioner** - for domain-specific distribution
6. **coalesce** - reduces partitions without shuffle
7. **repartition** - changes partition count with shuffle
8. **mapValues preserves partitioner** - map does not
9. **Data skew** - watch for hot keys, use salting if needed
10. **Pre-partition** for repeated joins on same key

## Time
~35 minutes

## Next
Continue to [kata-06-shuffling](../kata-06-shuffling/)
