# Kata 02: RDD Basics - The Foundation

## Goal
Master RDDs (Resilient Distributed Datasets), the fundamental data abstraction in Spark that everything else builds upon.

## What is an RDD?

```
RDD = Resilient + Distributed + Dataset
═══════════════════════════════════════════════════════════════════════════════

  RESILIENT                 DISTRIBUTED                 DATASET
  ─────────                 ───────────                 ───────
  Fault-tolerant            Spread across               Collection of
  through lineage           cluster nodes               typed elements

  If a partition fails,     Each partition can          RDD[Int], RDD[String],
  Spark rebuilds it         be processed on a           RDD[Person], etc.
  from the recipe           different machine


PHYSICAL LAYOUT OF AN RDD:
─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────┐
  │                              RDD[Int]                                   │
  │                                                                         │
  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
  │  │ Partition 0 │  │ Partition 1 │  │ Partition 2 │  │ Partition 3 │   │
  │  │  [1, 2, 3]  │  │  [4, 5, 6]  │  │  [7, 8, 9]  │  │ [10, 11, 12]│   │
  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘   │
  │         │                │                │                │           │
  │         ▼                ▼                ▼                ▼           │
  │      Worker 1         Worker 2         Worker 1         Worker 3       │
  │      (Node A)         (Node B)         (Node A)         (Node C)       │
  │                                                                         │
  └─────────────────────────────────────────────────────────────────────────┘

  Key insight: Partitions are the unit of parallelism in Spark.
  More partitions = more potential parallelism (up to available cores)
```

## The Five Properties of an RDD

Every RDD is defined by five properties:

```
THE FIVE RDD PROPERTIES
═══════════════════════════════════════════════════════════════════════════════

  1. A list of PARTITIONS
     ─────────────────────────────────────────────────────────────────────────
     rdd.partitions         // Array of Partition objects
     rdd.getNumPartitions   // Number of partitions

  2. A function for computing each SPLIT (partition)
     ─────────────────────────────────────────────────────────────────────────
     How to compute elements in each partition from parent RDD(s)

  3. A list of DEPENDENCIES on other RDDs
     ─────────────────────────────────────────────────────────────────────────
     rdd.dependencies       // What RDDs this depends on
     This forms the "lineage" - the recipe to recreate data

  4. (Optional) A PARTITIONER for key-value RDDs
     ─────────────────────────────────────────────────────────────────────────
     rdd.partitioner        // Some(HashPartitioner) or None
     Determines which partition a key goes to

  5. (Optional) PREFERRED LOCATIONS for each partition
     ─────────────────────────────────────────────────────────────────────────
     rdd.preferredLocations(partition)
     Where the data lives (for data locality optimization)


VISUALIZING THE FIVE PROPERTIES:
─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  RDD[String] (e.g., lines from a text file)                            │
  │                                                                         │
  │  1. Partitions: [P0, P1, P2, P3]                                       │
  │                                                                         │
  │  2. Compute function: (partition) => read lines from HDFS block        │
  │                                                                         │
  │  3. Dependencies: None (this is a source RDD)                          │
  │                                                                         │
  │  4. Partitioner: None (not a key-value RDD)                            │
  │                                                                         │
  │  5. Preferred locations:                                                │
  │     P0 -> [node1, node2]   (HDFS block replicas)                       │
  │     P1 -> [node2, node3]                                                │
  │     P2 -> [node1, node3]                                                │
  │     P3 -> [node3, node4]                                                │
  └─────────────────────────────────────────────────────────────────────────┘
```

## Creating RDDs

```scala
// ═══════════════════════════════════════════════════════════════════════════
// METHOD 1: parallelize() - From a local collection
// ═══════════════════════════════════════════════════════════════════════════

// Basic - Spark chooses partition count
val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))

// Explicit partition count
val rdd2 = sc.parallelize(1 to 1000, numSlices = 10)

// From any Scala collection
val rdd3 = sc.parallelize(Seq("a", "b", "c"))
val rdd4 = sc.parallelize(Array(1.0, 2.0, 3.0))


// ═══════════════════════════════════════════════════════════════════════════
// METHOD 2: textFile() - From external storage
// ═══════════════════════════════════════════════════════════════════════════

// Local file
val lines = sc.textFile("file:///path/to/local/file.txt")

// HDFS file
val hdfsLines = sc.textFile("hdfs:///data/logs/app.log")

// S3 file
val s3Lines = sc.textFile("s3a://bucket/path/file.txt")

// With minimum partitions
val morePartitions = sc.textFile("file.txt", minPartitions = 10)

// Wildcards (all .txt files in directory)
val allLogs = sc.textFile("logs/*.txt")


// ═══════════════════════════════════════════════════════════════════════════
// METHOD 3: wholeTextFiles() - File name + content pairs
// ═══════════════════════════════════════════════════════════════════════════

// Returns RDD[(filename, fileContent)]
val filesRDD = sc.wholeTextFiles("config/")
// Good for many small files


// ═══════════════════════════════════════════════════════════════════════════
// METHOD 4: From transformations on existing RDDs
// ═══════════════════════════════════════════════════════════════════════════

val doubled = rdd1.map(_ * 2)        // New RDD from transformation
val filtered = rdd1.filter(_ > 3)    // Another new RDD
```

## Partitions Deep Dive

```
WHY PARTITIONS MATTER
═══════════════════════════════════════════════════════════════════════════════

  Partitions = Units of parallelism

  ┌───────────────────────────────────────────────────────────────────────────┐
  │                                                                           │
  │   TOO FEW PARTITIONS              TOO MANY PARTITIONS                    │
  │   ───────────────────             ────────────────────                   │
  │                                                                           │
  │   [────────────────]              [─][─][─][─][─][─][─][─]               │
  │   [────────────────]              [─][─][─][─][─][─][─][─]               │
  │         ...                              ...                              │
  │                                                                           │
  │   Problems:                       Problems:                               │
  │   • Underutilized cores           • Scheduling overhead                  │
  │   • Memory pressure               • Small file problem on write          │
  │   • OOM risk                      • Inefficient I/O                      │
  │                                                                           │
  │   GUIDELINE: 2-4 partitions per CPU core                                 │
  │   GUIDELINE: 100MB - 1GB per partition                                   │
  │                                                                           │
  └───────────────────────────────────────────────────────────────────────────┘


CONTROLLING PARTITION COUNT:
─────────────────────────────────────────────────────────────────────────────

  At creation:
  sc.parallelize(data, numSlices = 8)
  sc.textFile("file", minPartitions = 8)

  After creation:
  rdd.repartition(10)   // Shuffle to get exactly 10 partitions
  rdd.coalesce(4)       // Reduce partitions (no shuffle if reducing)


INSPECTING PARTITIONS:
─────────────────────────────────────────────────────────────────────────────

  rdd.getNumPartitions           // Get partition count
  rdd.partitions.length          // Same thing

  // See what's in each partition
  rdd.glom().collect()           // Array[Array[T]] - elements per partition

  // Example:
  sc.parallelize(1 to 10, 3).glom().collect()
  // Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9, 10))
```

## Lineage - The Key to Fault Tolerance

```
WHAT IS LINEAGE?
═══════════════════════════════════════════════════════════════════════════════

  Lineage = The "recipe" to recreate an RDD from its source

  Instead of replicating data like traditional systems,
  Spark remembers how to recompute it.


LINEAGE EXAMPLE:
─────────────────────────────────────────────────────────────────────────────

  val lines = sc.textFile("logs.txt")      // Source RDD
  val errors = lines.filter(_.contains("ERROR"))
  val words = errors.flatMap(_.split(" "))
  val count = words.count()                 // Action triggers execution


  Lineage graph (DAG):
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                                                                         │
  │   textFile("logs.txt")                                                 │
  │         │                                                               │
  │         │  HadoopRDD                                                   │
  │         ▼                                                               │
  │   ┌───────────┐                                                        │
  │   │  lines    │  MapPartitionsRDD                                      │
  │   └─────┬─────┘                                                        │
  │         │                                                               │
  │         │  filter(_.contains("ERROR"))                                 │
  │         ▼                                                               │
  │   ┌───────────┐                                                        │
  │   │  errors   │  MapPartitionsRDD                                      │
  │   └─────┬─────┘                                                        │
  │         │                                                               │
  │         │  flatMap(_.split(" "))                                       │
  │         ▼                                                               │
  │   ┌───────────┐                                                        │
  │   │  words    │  MapPartitionsRDD                                      │
  │   └─────┬─────┘                                                        │
  │         │                                                               │
  │         │  count()                                                      │
  │         ▼                                                               │
  │      RESULT                                                             │
  │                                                                         │
  └─────────────────────────────────────────────────────────────────────────┘


VIEWING LINEAGE:
─────────────────────────────────────────────────────────────────────────────

  // toDebugString shows the lineage
  println(words.toDebugString)

  // Output:
  (2) MapPartitionsRDD[3] at flatMap
   |  MapPartitionsRDD[2] at filter
   |  MapPartitionsRDD[1] at textFile
   |  HadoopRDD[0] at textFile


FAULT TOLERANCE THROUGH LINEAGE:
─────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────────────┐
  │                                                                         │
  │   Normal execution:                                                     │
  │   ─────────────────                                                     │
  │   P0 ──► filter ──► flatMap ──► result                                 │
  │   P1 ──► filter ──► flatMap ──► result                                 │
  │   P2 ──► filter ──► flatMap ──► result   ← This node crashes!         │
  │   P3 ──► filter ──► flatMap ──► result                                 │
  │                                                                         │
  │   Recovery:                                                             │
  │   ─────────                                                             │
  │   Spark sees P2's result is lost.                                      │
  │   It replays the lineage for P2 only:                                  │
  │   1. Re-read P2's data from source                                     │
  │   2. Apply filter                                                       │
  │   3. Apply flatMap                                                      │
  │   4. Continue with recovered result                                     │
  │                                                                         │
  │   No replication needed! Just re-execute the recipe.                   │
  │                                                                         │
  └─────────────────────────────────────────────────────────────────────────┘
```

## Key RDD Properties

```
RDD CHARACTERISTICS TABLE
═══════════════════════════════════════════════════════════════════════════════

┌────────────────┬────────────────────────────────────────────────────────────┐
│  Property      │  Description                                               │
├────────────────┼────────────────────────────────────────────────────────────┤
│  IMMUTABLE     │  Once created, cannot be changed. Transformations create  │
│                │  new RDDs. This enables safe parallel processing.         │
├────────────────┼────────────────────────────────────────────────────────────┤
│  DISTRIBUTED   │  Data is spread across cluster nodes. Each partition can  │
│                │  be on a different machine.                                │
├────────────────┼────────────────────────────────────────────────────────────┤
│  FAULT-        │  If a partition is lost, Spark rebuilds it using lineage. │
│  TOLERANT      │  No need to replicate data.                               │
├────────────────┼────────────────────────────────────────────────────────────┤
│  LAZY          │  Transformations don't execute immediately. They build    │
│                │  a plan that executes when an action is called.           │
├────────────────┼────────────────────────────────────────────────────────────┤
│  TYPED         │  RDD[T] knows its element type at compile time.           │
│                │  RDD[Int], RDD[String], RDD[(K, V)]                       │
├────────────────┼────────────────────────────────────────────────────────────┤
│  IN-MEMORY     │  Can be cached in memory for fast repeated access.        │
│                │  (Optional - controlled by persist/cache)                  │
└────────────────┴────────────────────────────────────────────────────────────┘
```

## RDD vs DataFrame vs Dataset

```
WHEN TO USE WHAT
═══════════════════════════════════════════════════════════════════════════════

┌─────────────────┬──────────────────┬──────────────────┬──────────────────┐
│                 │       RDD        │    DataFrame     │     Dataset      │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Type Safety     │ Compile-time     │ Runtime only     │ Compile-time     │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Optimization    │ Manual           │ Catalyst         │ Catalyst         │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Serialization   │ Java/Kryo        │ Tungsten         │ Tungsten         │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ API Style       │ Functional       │ SQL-like         │ Both             │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Schema          │ None             │ Yes (Row)        │ Yes (case class) │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Use Case        │ Low-level        │ SQL/Analytics    │ Type-safe ETL    │
│                 │ control          │                  │                  │
└─────────────────┴──────────────────┴──────────────────┴──────────────────┘

  USE RDD WHEN:
  • You need fine-grained control over physical data layout
  • Working with unstructured data
  • Implementing custom partitioning
  • Interacting with legacy Spark code
  • Learning Spark fundamentals (like this kata!)

  USE DataFrame/Dataset WHEN:
  • Working with structured data
  • Want automatic optimizations
  • Using Spark SQL
  • Most production workloads (90%+ of cases)
```

## Debugging RDDs

```scala
// ═══════════════════════════════════════════════════════════════════════════
// USEFUL DEBUG METHODS
// ═══════════════════════════════════════════════════════════════════════════

val rdd = sc.parallelize(1 to 100, 4)

// View lineage
println(rdd.toDebugString)

// Get partition count
println(s"Partitions: ${rdd.getNumPartitions}")

// See partition contents (careful with large RDDs!)
rdd.glom().collect().zipWithIndex.foreach { case (data, idx) =>
  println(s"Partition $idx: ${data.mkString(", ")}")
}

// Get first few elements
rdd.take(5).foreach(println)

// Sample data
rdd.sample(withReplacement = false, fraction = 0.1).collect()

// Count elements
println(s"Count: ${rdd.count()}")
```

## Common Pitfalls

```
PITFALL 1: Collecting Large RDDs
─────────────────────────────────────────────────────────────────────────────
  // BAD - pulls all data to driver, can cause OOM
  val allData = rdd.collect()

  // GOOD - take only what you need
  val sample = rdd.take(100)
  val sample2 = rdd.sample(false, 0.01).collect()


PITFALL 2: Creating RDDs in Loops
─────────────────────────────────────────────────────────────────────────────
  // BAD - creates many small RDDs
  for (i <- 1 to 100) {
    val rdd = sc.parallelize(Seq(i))
    // ... process
  }

  // GOOD - create one RDD
  val rdd = sc.parallelize(1 to 100)


PITFALL 3: Not Understanding Lazy Evaluation
─────────────────────────────────────────────────────────────────────────────
  val rdd = sc.textFile("huge_file.txt")
    .filter(_.contains("ERROR"))
    .map(_.split(" "))

  // Nothing has happened yet! No data read, no filtering.
  // Only when you call an action:
  rdd.count()  // NOW everything executes


PITFALL 4: Wrong Partition Count
─────────────────────────────────────────────────────────────────────────────
  // BAD - 1 partition for 100GB
  val rdd = sc.textFile("huge_100gb_file.txt", 1)

  // GOOD - appropriate partitions (1GB chunks)
  val rdd = sc.textFile("huge_100gb_file.txt", 100)
```

## Instructions

1. **Read** this README thoroughly - understand RDD fundamentals
2. **Open** `src/main/scala/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-02-rdd-basics && sbt run`
4. **Verify** your understanding with the exercises
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **RDD** = Resilient Distributed Dataset - the core abstraction
2. **Partitions** are the unit of parallelism (2-4 per CPU core)
3. **Lineage** tracks transformation history for fault tolerance
4. **Immutable** - transformations create new RDDs
5. **Lazy** - nothing executes until an action is called
6. **toDebugString** shows the lineage/DAG
7. **glom()** lets you see partition contents
8. Use DataFrame/Dataset for most work; RDD for low-level control

## Time
~25 minutes

## Next
Continue to [kata-03-transformations](../kata-03-transformations/)
