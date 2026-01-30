# Kata 07: Narrow vs Wide Transformations - Understanding Dependencies

## Goal
Master the distinction between narrow and wide transformations, understand how they affect stage boundaries and pipelining, and learn to optimize computation graphs by maximizing narrow transformation chains.

## The Core Concept

```
NARROW VS WIDE: THE FUNDAMENTAL DISTINCTION
═══════════════════════════════════════════════════════════════════════════════

  The difference comes down to ONE question:
  "How many PARENT partitions does each CHILD partition depend on?"

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  NARROW TRANSFORMATION                WIDE TRANSFORMATION                   │
  │  ─────────────────────                ───────────────────                   │
  │                                                                             │
  │  Each output partition depends        Each output partition depends         │
  │  on AT MOST ONE input partition       on ALL input partitions               │
  │                                                                             │
  │  Parent        Child                  Parent        Child                   │
  │  ┌─────┐      ┌─────┐                ┌─────┐      ┌─────┐                  │
  │  │  0  │─────►│  0  │                │  0  │──┬──►│  0  │                  │
  │  └─────┘      └─────┘                └─────┘  │   └─────┘                  │
  │  ┌─────┐      ┌─────┐                ┌─────┐  │   ┌─────┐                  │
  │  │  1  │─────►│  1  │                │  1  │──┼──►│  1  │                  │
  │  └─────┘      └─────┘                └─────┘  │   └─────┘                  │
  │  ┌─────┐      ┌─────┐                ┌─────┐  │   ┌─────┐                  │
  │  │  2  │─────►│  2  │                │  2  │──┴──►│  2  │                  │
  │  └─────┘      └─────┘                └─────┘      └─────┘                  │
  │                                                                             │
  │  One-to-One                          Many-to-Many                          │
  │  (or Many-to-One for union/coalesce) (Shuffle Required!)                   │
  │                                                                             │
  │  Examples:                            Examples:                             │
  │  map, filter, flatMap                 groupByKey, reduceByKey               │
  │  mapPartitions, union                 join, distinct, sortByKey             │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Visual Deep Dive: Narrow Transformations

```
NARROW TRANSFORMATIONS IN DETAIL
═══════════════════════════════════════════════════════════════════════════════


  map() - One-to-One element transformation
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Partition 0        map(x => x * 2)        Partition 0                     │
  │  ┌─────────────┐                          ┌─────────────┐                  │
  │  │ [1, 2, 3]   │ ────────────────────────►│ [2, 4, 6]   │                  │
  │  └─────────────┘                          └─────────────┘                  │
  │                                                                             │
  │  Partition 1        map(x => x * 2)        Partition 1                     │
  │  ┌─────────────┐                          ┌─────────────┐                  │
  │  │ [4, 5, 6]   │ ────────────────────────►│ [8, 10, 12] │                  │
  │  └─────────────┘                          └─────────────┘                  │
  │                                                                             │
  │  Each output partition comes from exactly ONE input partition              │
  │  No communication between partitions needed                                │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  filter() - Selective one-to-one
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Partition 0      filter(_ > 3)           Partition 0                      │
  │  ┌─────────────┐                          ┌─────────────┐                  │
  │  │ [1, 4, 2, 5]│ ────────────────────────►│ [4, 5]      │                  │
  │  └─────────────┘                          └─────────────┘                  │
  │                                                                             │
  │  Partition 1      filter(_ > 3)           Partition 1                      │
  │  ┌─────────────┐                          ┌─────────────┐                  │
  │  │ [3, 6, 1, 7]│ ────────────────────────►│ [6, 7]      │                  │
  │  └─────────────┘                          └─────────────┘                  │
  │                                                                             │
  │  Output may have fewer elements, but same partition structure              │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  flatMap() - One-to-many within partition
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Partition 0     flatMap(_.split(" "))     Partition 0                     │
  │  ┌─────────────┐                          ┌─────────────────────┐          │
  │  │ ["a b c",   │                          │ ["a", "b", "c",     │          │
  │  │  "d e"]     │ ────────────────────────►│  "d", "e"]          │          │
  │  └─────────────┘                          └─────────────────────┘          │
  │                                                                             │
  │  More elements produced, but still within same partition                   │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  union() - Special narrow: many partitions to many partitions
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  RDD1                           Result (union)                             │
  │  ┌─────────────┐               ┌─────────────┐                             │
  │  │ Partition 0 │ ─────────────►│ Partition 0 │  (from RDD1 P0)            │
  │  └─────────────┘               └─────────────┘                             │
  │  ┌─────────────┐               ┌─────────────┐                             │
  │  │ Partition 1 │ ─────────────►│ Partition 1 │  (from RDD1 P1)            │
  │  └─────────────┘               └─────────────┘                             │
  │                                ┌─────────────┐                             │
  │  RDD2                          │ Partition 2 │  (from RDD2 P0)            │
  │  ┌─────────────┐               └─────────────┘                             │
  │  │ Partition 0 │ ─────────────►┌─────────────┐                             │
  │  └─────────────┘               │ Partition 3 │  (from RDD2 P1)            │
  │  ┌─────────────┐               └─────────────┘                             │
  │  │ Partition 1 │ ──────────────────────┘                                   │
  │  └─────────────┘                                                           │
  │                                                                             │
  │  Each output partition depends on exactly ONE input partition              │
  │  Just concatenates partition lists - no data movement!                     │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Visual Deep Dive: Wide Transformations

```
WIDE TRANSFORMATIONS IN DETAIL
═══════════════════════════════════════════════════════════════════════════════


  groupByKey() - All-to-all shuffle
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Input Partitions                 Output Partitions                        │
  │                                                                             │
  │  P0: [(a,1), (b,2)]              P0: [(a, [1,3])]                          │
  │  ┌─────────────────┐              ┌─────────────────┐                      │
  │  │  (a,1) → P0     │──┐       ┌──►│  (a, [1,3])     │                      │
  │  │  (b,2) → P1     │──┼───┐   │   └─────────────────┘                      │
  │  └─────────────────┘  │   │   │                                            │
  │                       │   │   │   P1: [(b, [2,4])]                         │
  │  P1: [(a,3), (b,4)]   │   │   │   ┌─────────────────┐                      │
  │  ┌─────────────────┐  │   │   │   │  (b, [2,4])     │                      │
  │  │  (a,3) → P0     │──┘   │   │   └─────────────────┘                      │
  │  │  (b,4) → P1     │──────┼───┼──►        ▲                                │
  │  └─────────────────┘      │   │           │                                │
  │                           │   │           │                                │
  │                           └───┴───────────┘                                │
  │                                                                             │
  │  Every input partition may contribute to ANY output partition              │
  │  SHUFFLE REQUIRED - data must move across network                          │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  join() - Two-way all-to-all
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  RDD1                    RDD2                    Result                    │
  │  ┌─────────────┐        ┌─────────────┐        ┌─────────────────────┐     │
  │  │ P0: (a,1)   │───┐    │ P0: (a,X)   │───┐    │ P0: (a,(1,X))       │     │
  │  │     (b,2)   │───┼────│     (c,Z)   │───┼───►│     (a,(3,X))       │     │
  │  └─────────────┘   │    └─────────────┘   │    └─────────────────────┘     │
  │                    │                      │                                 │
  │  ┌─────────────┐   │    ┌─────────────┐   │    ┌─────────────────────┐     │
  │  │ P1: (a,3)   │───┼────│ P1: (b,Y)   │───┼───►│ P1: (b,(2,Y))       │     │
  │  │     (c,4)   │───┘    │             │───┘    │     (c,(4,Z))       │     │
  │  └─────────────┘        └─────────────┘        └─────────────────────┘     │
  │                                                                             │
  │  Both RDDs shuffled to bring matching keys together                        │
  │  (Unless pre-partitioned with same partitioner!)                           │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  sortByKey() - Range partition then sort
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Input (unsorted)                 Output (sorted)                          │
  │                                                                             │
  │  P0: [5, 1, 8]                   P0: [1, 2, 3]    (keys 1-3)              │
  │  P1: [3, 9, 2]                   P1: [5, 6, 8]    (keys 4-7)              │
  │  P2: [6, 4, 7]                   P2: [9]          (keys 8+)               │
  │                                                                             │
  │  1. Sample data to find range boundaries                                   │
  │  2. Shuffle each element to appropriate range partition                    │
  │  3. Sort within each partition                                             │
  │                                                                             │
  │  SHUFFLE REQUIRED - redistributes data by key ranges                       │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## The Complete Classification

```
TRANSFORMATION CLASSIFICATION
═══════════════════════════════════════════════════════════════════════════════

┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  NARROW TRANSFORMATIONS (No Shuffle)                                         │
│  ─────────────────────────────────────────────────────────────────────────── │
│                                                                              │
│  ┌────────────────────┬─────────────────────────────────────────────────┐   │
│  │ Transformation     │ Description                                     │   │
│  ├────────────────────┼─────────────────────────────────────────────────┤   │
│  │ map                │ Transform each element                          │   │
│  │ filter             │ Keep matching elements                          │   │
│  │ flatMap            │ One-to-many transformation                      │   │
│  │ mapPartitions      │ Transform entire partition at once              │   │
│  │ mapPartitionsWithIndex │ With partition index                        │   │
│  │ mapValues          │ Transform values only (preserves partitioner)   │   │
│  │ flatMapValues      │ One-to-many on values                           │   │
│  │ union              │ Combine partition lists                         │   │
│  │ coalesce(n, false) │ Merge partitions without shuffle                │   │
│  │ sample             │ Random sampling                                 │   │
│  │ pipe               │ Pipe through external process                   │   │
│  │ glom               │ Collect partition to array                      │   │
│  │ cartesian*         │ Narrow within partitions                        │   │
│  └────────────────────┴─────────────────────────────────────────────────┘   │
│                                                                              │
│                                                                              │
│  WIDE TRANSFORMATIONS (Shuffle Required)                                     │
│  ─────────────────────────────────────────────────────────────────────────── │
│                                                                              │
│  ┌────────────────────┬─────────────────────────────────────────────────┐   │
│  │ Transformation     │ Description                                     │   │
│  ├────────────────────┼─────────────────────────────────────────────────┤   │
│  │ groupByKey         │ Group values by key                             │   │
│  │ reduceByKey        │ Aggregate values by key (with combiner)         │   │
│  │ aggregateByKey     │ Aggregate with different types                  │   │
│  │ combineByKey       │ General aggregation by key                      │   │
│  │ sortByKey          │ Sort by key (range partitioning)                │   │
│  │ sortBy             │ Sort by function                                │   │
│  │ join               │ Join two RDDs by key                            │   │
│  │ leftOuterJoin      │ Left outer join                                 │   │
│  │ rightOuterJoin     │ Right outer join                                │   │
│  │ fullOuterJoin      │ Full outer join                                 │   │
│  │ cogroup            │ Group matching keys from multiple RDDs          │   │
│  │ distinct           │ Remove duplicates                               │   │
│  │ intersection       │ Elements in both RDDs                           │   │
│  │ subtract           │ Elements in first but not second                │   │
│  │ subtractByKey      │ Keys in first but not second                    │   │
│  │ repartition        │ Redistribute with shuffle                       │   │
│  │ coalesce(n, true)  │ With shuffle for balanced partitions            │   │
│  │ repartitionAndSort │ Repartition and sort within partitions          │   │
│  └────────────────────┴─────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Stage Boundaries and Pipelining

```
HOW SPARK CREATES STAGES
═══════════════════════════════════════════════════════════════════════════════

  Narrow transformations can be PIPELINED in the same stage.
  Wide transformations create STAGE BOUNDARIES.

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Code:                                                                      │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  val result = sc.textFile("data.txt")     // Source                        │
  │    .flatMap(_.split(" "))                  // Narrow                        │
  │    .map(word => (word, 1))                 // Narrow                        │
  │    .reduceByKey(_ + _)                     // WIDE - shuffle!              │
  │    .filter(_._2 > 10)                      // Narrow                        │
  │    .sortByKey()                            // WIDE - shuffle!              │
  │                                                                             │
  │                                                                             │
  │  Stage Boundaries:                                                          │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  ╔═══════════════════════════════════════════════════════════════════════╗ │
  │  ║  STAGE 0                                                              ║ │
  │  ║  ─────────────────────────────────────────────────────────────────────║ │
  │  ║  textFile → flatMap → map → (shuffle write for reduceByKey)          ║ │
  │  ║                                                                       ║ │
  │  ║  All narrow ops PIPELINED - data flows through without materializing ║ │
  │  ╚═══════════════════════════════════════════════════════════════════════╝ │
  │                              │                                              │
  │                              │ SHUFFLE                                      │
  │                              ▼                                              │
  │  ╔═══════════════════════════════════════════════════════════════════════╗ │
  │  ║  STAGE 1                                                              ║ │
  │  ║  ─────────────────────────────────────────────────────────────────────║ │
  │  ║  (shuffle read) → reduceByKey → filter → (shuffle write for sort)    ║ │
  │  ║                                                                       ║ │
  │  ║  Narrow ops after reduceByKey PIPELINED together                      ║ │
  │  ╚═══════════════════════════════════════════════════════════════════════╝ │
  │                              │                                              │
  │                              │ SHUFFLE                                      │
  │                              ▼                                              │
  │  ╔═══════════════════════════════════════════════════════════════════════╗ │
  │  ║  STAGE 2                                                              ║ │
  │  ║  ─────────────────────────────────────────────────────────────────────║ │
  │  ║  (shuffle read) → sortByKey                                           ║ │
  │  ╚═══════════════════════════════════════════════════════════════════════╝ │
  │                                                                             │
  │  Total: 3 stages, 2 shuffles                                               │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Pipelining Explained

```
THE POWER OF PIPELINING
═══════════════════════════════════════════════════════════════════════════════

  Narrow transformations are PIPELINED - executed in a single pass:

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  WITHOUT PIPELINING (hypothetical):                                         │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  [Read entire file] → [Store intermediate]                                 │
  │                              ↓                                              │
  │  [flatMap ALL] → [Store intermediate]                                      │
  │                              ↓                                              │
  │  [map ALL] → [Store intermediate]                                          │
  │                              ↓                                              │
  │  [filter ALL] → [Store result]                                             │
  │                                                                             │
  │  Memory needed: Entire dataset × 4!                                        │
  │  Passes through data: 4                                                    │
  │                                                                             │
  │                                                                             │
  │  WITH PIPELINING (how Spark works):                                         │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  For each record:                                                           │
  │    [Read line] → [flatMap] → [map] → [filter] → [output]                   │
  │                                                                             │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ "hello world" → ["hello", "world"] → [("hello",1), ("world",1)]    │   │
  │  │              → filter → output directly to shuffle buffer           │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  │  Memory needed: One record at a time!                                      │
  │  Passes through data: 1                                                    │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  TASK EXECUTION WITHIN A STAGE:
  ─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Task (one per partition):                                                  │
  │                                                                             │
  │  while (hasNextRecord) {                                                    │
  │    record = readNext()                                                      │
  │    // All narrow transformations fused into single loop                    │
  │    result = filter(map(flatMap(record)))                                   │
  │    writeToShuffleOrOutput(result)                                          │
  │  }                                                                          │
  │                                                                             │
  │  This is why narrow transformations are "free" compared to wide ones!      │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Understanding Dependencies

```
DEPENDENCY TYPES IN SPARK
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  NARROW DEPENDENCY (OneToOneDependency, PruneDependency)                   │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • Parent partition used by at most ONE child partition                    │
  │  • No data sharing between partitions                                      │
  │  • Fault recovery: Only rebuild the specific partition                     │
  │                                                                             │
  │  Parent RDD     Child RDD                                                  │
  │  ┌───────┐     ┌───────┐                                                   │
  │  │ P0    │────►│ P0    │   If P0 fails, only rebuild P0                   │
  │  ├───────┤     ├───────┤   from parent's P0                               │
  │  │ P1    │────►│ P1    │                                                   │
  │  ├───────┤     ├───────┤                                                   │
  │  │ P2    │────►│ P2    │                                                   │
  │  └───────┘     └───────┘                                                   │
  │                                                                             │
  │                                                                             │
  │  WIDE DEPENDENCY (ShuffleDependency)                                        │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • Parent partition used by MULTIPLE child partitions                      │
  │  • Data must be shuffled across the network                                │
  │  • Fault recovery: May need to rebuild ALL parent partitions!              │
  │                                                                             │
  │  Parent RDD     Child RDD                                                  │
  │  ┌───────┐     ┌───────┐                                                   │
  │  │ P0    │──┬─►│ P0    │   If P0 fails, may need data from                │
  │  ├───────┤  │  ├───────┤   ALL parent partitions!                         │
  │  │ P1    │──┼─►│ P1    │                                                   │
  │  ├───────┤  │  ├───────┤   (Unless shuffle files cached)                  │
  │  │ P2    │──┴─►│ P2    │                                                   │
  │  └───────┘     └───────┘                                                   │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Examining dependencies programmatically
// ═══════════════════════════════════════════════════════════════════════════

val rdd = sc.parallelize(1 to 100, 4)
val mapped = rdd.map(_ * 2)
val filtered = mapped.filter(_ > 50)
val grouped = filtered.map(x => (x % 10, x)).groupByKey()

// Check dependency types
mapped.dependencies.foreach { dep =>
  println(s"mapped dependency: ${dep.getClass.getSimpleName}")
  // OneToOneDependency - narrow
}

grouped.dependencies.foreach { dep =>
  println(s"grouped dependency: ${dep.getClass.getSimpleName}")
  // ShuffleDependency - wide
}


// ═══════════════════════════════════════════════════════════════════════════
// Viewing the full lineage
// ═══════════════════════════════════════════════════════════════════════════

println(grouped.toDebugString)
/*
(4) ShuffledRDD[4] at groupByKey at <console>:29 []
 +-(4) MapPartitionsRDD[3] at map at <console>:28 []
    |  MapPartitionsRDD[2] at filter at <console>:27 []
    |  MapPartitionsRDD[1] at map at <console>:26 []
    |  ParallelCollectionRDD[0] at parallelize at <console>:25 []

The "+-" indicates shuffle dependency (wide)
The "|" indicates narrow dependency
*/
```

## Optimization Strategies

```
OPTIMIZING WITH NARROW/WIDE KNOWLEDGE
═══════════════════════════════════════════════════════════════════════════════


  STRATEGY 1: Maximize narrow transformation chains
  ─────────────────────────────────────────────────────────────────────────────

  // BAD: Shuffle in the middle breaks pipelining
  rdd.map(f1)
     .reduceByKey(+)     // Shuffle!
     .map(f2)            // New stage
     .reduceByKey(+)     // Another shuffle!
     .map(f3)            // Another new stage

  // BETTER: Combine maps before shuffle
  rdd.map(f1 andThen f2 andThen f3)
     .reduceByKey(+)     // Single shuffle


  STRATEGY 2: Filter before shuffle
  ─────────────────────────────────────────────────────────────────────────────

  // BAD: Shuffle then filter
  rdd.groupByKey()         // Shuffle ALL data
     .filter(condition)     // Then filter

  // GOOD: Filter before shuffle
  rdd.filter(condition)    // Filter first (narrow)
     .groupByKey()         // Shuffle less data


  STRATEGY 3: Use mapValues to preserve partitioner
  ─────────────────────────────────────────────────────────────────────────────

  val partitioned = rdd.partitionBy(new HashPartitioner(100))

  // BAD: map loses partitioner, next groupBy shuffles
  partitioned.map { case (k, v) => (k, f(v)) }
             .groupByKey()  // Shuffle!

  // GOOD: mapValues preserves partitioner
  partitioned.mapValues(f)
             .groupByKey()  // No shuffle if same partitioner!


  STRATEGY 4: Understand coalesce behavior
  ─────────────────────────────────────────────────────────────────────────────

  // coalesce without shuffle - NARROW (but may cause uneven partitions)
  rdd.coalesce(10)  // Merges partitions locally

  // coalesce with shuffle - WIDE (but even distribution)
  rdd.coalesce(10, shuffle = true)  // Redistributes evenly

  // repartition - always WIDE
  rdd.repartition(10)  // Same as coalesce with shuffle=true
```

## Code Examples

```scala
// ═══════════════════════════════════════════════════════════════════════════
// Example 1: Counting stage boundaries
// ═══════════════════════════════════════════════════════════════════════════

val result = sc.textFile("logs.txt")    // Stage 0 starts
  .filter(_.contains("ERROR"))           // Narrow - same stage
  .map(_.split("\t")(0))                 // Narrow - same stage
  .map(ts => (ts, 1))                    // Narrow - same stage
  .reduceByKey(_ + _)                    // WIDE - Stage 1 starts
  .filter(_._2 > 100)                    // Narrow - same stage
  .sortByKey()                           // WIDE - Stage 2 starts

// 3 stages total (2 shuffles)


// ═══════════════════════════════════════════════════════════════════════════
// Example 2: Optimizing a word count
// ═══════════════════════════════════════════════════════════════════════════

// Original - works but let's trace the stages
val counts = sc.textFile("book.txt")
  .flatMap(_.split("\\s+"))              // Narrow
  .map(_.toLowerCase)                     // Narrow
  .map(word => (word, 1))                 // Narrow
  .reduceByKey(_ + _)                     // WIDE
  .filter(_._2 > 5)                       // Narrow

// Optimized - combine map operations
val optimizedCounts = sc.textFile("book.txt")
  .flatMap(line => line.split("\\s+").map(_.toLowerCase))  // Combined
  .map(word => (word, 1))
  .reduceByKey(_ + _)
  .filter(_._2 > 5)

// Same number of stages, but fewer function calls per element


// ═══════════════════════════════════════════════════════════════════════════
// Example 3: Join optimization
// ═══════════════════════════════════════════════════════════════════════════

val users = userRdd.partitionBy(new HashPartitioner(100))
val orders = orderRdd.partitionBy(new HashPartitioner(100))
val products = productRdd.partitionBy(new HashPartitioner(100))

// All these are shuffle-free because of co-partitioning!
val userOrders = users.join(orders)       // No shuffle
val enriched = userOrders.join(products)  // No shuffle

// Use mapValues to keep the partitioner
val processed = enriched.mapValues(processRecord)  // Preserves partitioner


// ═══════════════════════════════════════════════════════════════════════════
// Example 4: Avoiding unnecessary shuffles
// ═══════════════════════════════════════════════════════════════════════════

// BAD: distinct causes shuffle even if data is already unique
val alreadyUnique = uniqueRdd.distinct()  // Unnecessary shuffle!

// GOOD: Skip distinct if you know data is unique
val result = uniqueRdd  // No shuffle needed

// BAD: sortByKey when order doesn't matter
val badSort = rdd.sortByKey().take(10)  // Shuffles ALL data!

// GOOD: Use take without sorting, or takeOrdered
val top10 = rdd.takeOrdered(10)  // More efficient for small results
```

## Common Pitfalls

```
PITFALL 1: Thinking union always shuffles
─────────────────────────────────────────────────────────────────────────────

  // union is NARROW - just concatenates partition lists
  val combined = rdd1.union(rdd2)  // No shuffle!

  // If you need balanced partitions after union, THEN repartition
  val balanced = rdd1.union(rdd2).repartition(targetPartitions)


PITFALL 2: Not realizing coalesce can be narrow
─────────────────────────────────────────────────────────────────────────────

  // Reducing partitions without shuffle (narrow)
  val fewer = rdd.coalesce(10)  // Merges locally

  // Increasing partitions requires shuffle (wide)
  val more = rdd.coalesce(100, shuffle = true)  // Must shuffle


PITFALL 3: Breaking pipelines unnecessarily
─────────────────────────────────────────────────────────────────────────────

  // BAD: Forcing materialization breaks pipeline
  val step1 = rdd.map(f).collect()  // Collects to driver!
  val step2 = sc.parallelize(step1).filter(g)  // New RDD from scratch

  // GOOD: Keep transformations lazy
  val result = rdd.map(f).filter(g)  // Single pipeline


PITFALL 4: Using map instead of mapValues
─────────────────────────────────────────────────────────────────────────────

  val partitioned = pairs.partitionBy(new HashPartitioner(100))

  // BAD: Loses partitioner!
  val bad = partitioned.map { case (k, v) => (k, v + 1) }

  // GOOD: Preserves partitioner
  val good = partitioned.mapValues(_ + 1)


PITFALL 5: Multiple shuffles for related aggregations
─────────────────────────────────────────────────────────────────────────────

  // BAD: Two shuffles for related operations
  val sum = rdd.reduceByKey(_ + _)
  val count = rdd.mapValues(_ => 1).reduceByKey(_ + _)

  // GOOD: Single shuffle with aggregateByKey
  val sumAndCount = rdd.aggregateByKey((0.0, 0L))(
    (acc, v) => (acc._1 + v, acc._2 + 1),
    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )
```

## Best Practices

```
BEST PRACTICE 1: Chain narrow transformations
─────────────────────────────────────────────────────────────────────────────

  // Combine all narrow ops before any wide op
  val result = rdd
    .filter(condition)    // Narrow
    .map(transform)       // Narrow
    .flatMap(expand)      // Narrow
    .reduceByKey(combine) // Wide - only one shuffle


BEST PRACTICE 2: Use toDebugString to understand your DAG
─────────────────────────────────────────────────────────────────────────────

  println(result.toDebugString)
  // Count the "+-" lines - each is a shuffle
  // Fewer shuffles = better performance


BEST PRACTICE 3: Pre-partition for repeated operations
─────────────────────────────────────────────────────────────────────────────

  // Partition once, use many times
  val partitioned = rdd.partitionBy(new HashPartitioner(200)).cache()

  // All these are shuffle-free with the right operations
  val agg1 = partitioned.reduceByKey(_ + _)
  val agg2 = partitioned.mapValues(f).reduceByKey(_ + _)


BEST PRACTICE 4: Understand action implications
─────────────────────────────────────────────────────────────────────────────

  // Actions like count() and collect() don't add shuffles
  // But they do trigger all pending transformations

  val transformed = rdd.map(f).filter(g)  // Lazy
  transformed.count()  // Triggers execution of map and filter


BEST PRACTICE 5: Profile before optimizing
─────────────────────────────────────────────────────────────────────────────

  // Check Spark UI for actual stage counts and shuffle sizes
  // Focus optimization effort on stages with:
  // - High shuffle read/write
  // - Long duration
  // - Many tasks
```

## Instructions

1. **Read** this README thoroughly - understand narrow vs wide
2. **Open** `src/main/scala/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-07-narrow-wide-transformations && sbt run`
4. **Analyze**: Use toDebugString to verify your understanding
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **Narrow transformations** - one parent partition per child, pipelined
2. **Wide transformations** - multiple parents, require shuffle
3. **Pipelining** - narrow ops executed in single pass per record
4. **Stage boundaries** - created by wide transformations
5. **map, filter, flatMap** - always narrow
6. **groupByKey, reduceByKey, join** - always wide
7. **union** - narrow (just concatenates partitions)
8. **coalesce** - narrow when reducing, wide with shuffle=true
9. **mapValues preserves partitioner** - map does not
10. **toDebugString** - shows dependency structure

## Time
~30 minutes

## Next
Continue to [kata-08-caching-persistence](../kata-08-caching-persistence/)
