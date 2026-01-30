# Kata 04: Lazy Evaluation - The Art of Deferred Execution

## Goal
Master Spark's lazy evaluation model, understand how the DAG (Directed Acyclic Graph) is built, and learn when computation actually happens.

## The Core Concept

```
LAZY EVALUATION - SPARK'S FUNDAMENTAL EXECUTION MODEL
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │   TRANSFORMATION                           ACTION                           │
  │   ──────────────                           ──────                           │
  │                                                                             │
  │   val rdd1 = sc.parallelize(1 to 1000000)  // Nothing happens              │
  │   val rdd2 = rdd1.map(_ * 2)               // Nothing happens              │
  │   val rdd3 = rdd2.filter(_ > 100)          // Nothing happens              │
  │   val rdd4 = rdd3.map(_ + 1)               // Nothing happens              │
  │                                                                             │
  │   val result = rdd4.count()  // ← NOW everything executes!                 │
  │                                                                             │
  │   ANALOGY:                                                                  │
  │   Writing a recipe vs. cooking the meal                                     │
  │   Transformations write the recipe; actions cook it.                        │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Why Lazy Evaluation?

```
THE THREE PILLARS OF LAZY EVALUATION
═══════════════════════════════════════════════════════════════════════════════

  1. OPTIMIZATION
  ─────────────────────────────────────────────────────────────────────────────

     Spark sees the ENTIRE pipeline before executing anything.
     This enables powerful optimizations:

     ┌─────────────────────────────────────────────────────────────────────────┐
     │  YOUR CODE:                                                             │
     │  rdd.map(_ * 2).map(_ + 1).filter(_ > 10)                              │
     │                                                                         │
     │  WHAT SPARK CAN DO:                                                     │
     │  - Combine the two maps: map(x => (x * 2) + 1)                         │
     │  - Apply filter early if possible (predicate pushdown)                  │
     │  - Reorder operations for efficiency                                    │
     │                                                                         │
     │  WITHOUT LAZINESS:                                                      │
     │  Each operation would create a full intermediate RDD                    │
     │  No optimization opportunity                                            │
     └─────────────────────────────────────────────────────────────────────────┘


  2. EFFICIENCY - No Intermediate Materialization
  ─────────────────────────────────────────────────────────────────────────────

     EAGER EVALUATION (hypothetical):
     ┌─────────────────────────────────────────────────────────────────────────┐
     │                                                                         │
     │  rdd1     map       rdd2      filter     rdd3      map       rdd4      │
     │  [1M]  ─────────► [1M]    ─────────►  [500K]  ─────────►  [500K]       │
     │         STORE!            STORE!                STORE!                 │
     │                                                                         │
     │  Memory: 1M + 1M + 500K + 500K = 3M elements stored                    │
     │                                                                         │
     └─────────────────────────────────────────────────────────────────────────┘

     LAZY EVALUATION (Spark's approach):
     ┌─────────────────────────────────────────────────────────────────────────┐
     │                                                                         │
     │  rdd1 ──────► map ──────► filter ──────► map ──────► result            │
     │                                                                         │
     │         PIPELINED - NO INTERMEDIATE STORAGE!                           │
     │                                                                         │
     │  For each element: read → map → filter → map → output                  │
     │  Memory: Only current elements being processed                         │
     │                                                                         │
     └─────────────────────────────────────────────────────────────────────────┘


  3. FAULT TOLERANCE - Lineage as Recovery Recipe
  ─────────────────────────────────────────────────────────────────────────────

     The lazy DAG IS the recovery plan!

     ┌─────────────────────────────────────────────────────────────────────────┐
     │                                                                         │
     │  If a partition fails:                                                  │
     │                                                                         │
     │  textFile ──► filter ──► map ──► [P0] ✓                               │
     │                                   [P1] ✓                               │
     │                                   [P2] ✗ FAILED!                       │
     │                                   [P3] ✓                               │
     │                                                                         │
     │  Recovery: Replay P2's lineage only                                    │
     │                                                                         │
     │  textFile(P2) ──► filter ──► map ──► [P2] ✓ Recovered!                │
     │                                                                         │
     │  No replication needed. Just re-execute the recipe for P2.             │
     │                                                                         │
     └─────────────────────────────────────────────────────────────────────────┘
```

## The DAG - Directed Acyclic Graph

```
UNDERSTANDING THE DAG
═══════════════════════════════════════════════════════════════════════════════

  When you write transformations, Spark builds a DAG of RDDs:

  Code:
    val lines = sc.textFile("data.txt")
    val words = lines.flatMap(_.split(" "))
    val filtered = words.filter(_.length > 3)
    val pairs = filtered.map(w => (w, 1))
    val counts = pairs.reduceByKey(_ + _)


  DAG Visualization:
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │   ┌──────────────┐                                                         │
  │   │  HadoopRDD   │  textFile("data.txt")                                  │
  │   │   (lines)    │  Reads from HDFS/local file                            │
  │   └──────┬───────┘                                                         │
  │          │                                                                  │
  │          │ Narrow dependency (one-to-one)                                  │
  │          ▼                                                                  │
  │   ┌──────────────┐                                                         │
  │   │ MapPartitions│  flatMap(_.split(" "))                                 │
  │   │   (words)    │  Each line → multiple words                            │
  │   └──────┬───────┘                                                         │
  │          │                                                                  │
  │          │ Narrow dependency                                               │
  │          ▼                                                                  │
  │   ┌──────────────┐                                                         │
  │   │ MapPartitions│  filter(_.length > 3)                                  │
  │   │  (filtered)  │  Keep words with length > 3                            │
  │   └──────┬───────┘                                                         │
  │          │                                                                  │
  │          │ Narrow dependency                                               │
  │          ▼                                                                  │
  │   ┌──────────────┐                                                         │
  │   │ MapPartitions│  map(w => (w, 1))                                      │
  │   │   (pairs)    │  Create key-value pairs                                │
  │   └──────┬───────┘                                                         │
  │          │                                                                  │
  │          │ WIDE dependency (shuffle!)                                      │
  │          ▼                                                                  │
  │   ┌──────────────┐                                                         │
  │   │ ShuffledRDD  │  reduceByKey(_ + _)                                    │
  │   │   (counts)   │  Aggregate counts per word                             │
  │   └──────────────┘                                                         │
  │                                                                             │
  │   ══════════════════════════════════════════════════════════════════════   │
  │   STAGE BOUNDARY: Wide transformation creates a new stage                  │
  │   ══════════════════════════════════════════════════════════════════════   │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  The DAG enables:
  • Optimization across the entire pipeline
  • Stage creation for efficient execution
  • Lineage tracking for fault tolerance
```

## Proving Laziness with Side Effects

```scala
// ═══════════════════════════════════════════════════════════════════════════
// PROOF 1: Using a counter
// ═══════════════════════════════════════════════════════════════════════════

var transformationCount = 0

val rdd = sc.parallelize(1 to 100).map { x =>
  transformationCount += 1  // Side effect (runs on executors)
  x * 2
}

println(s"After map: $transformationCount")  // Prints 0! Nothing executed yet.

rdd.collect()  // Action triggers execution

// Note: transformationCount on driver may still be 0 due to serialization!
// The counter increments happen on executors, not the driver.


// ═══════════════════════════════════════════════════════════════════════════
// PROOF 2: Using print statements
// ═══════════════════════════════════════════════════════════════════════════

val rdd = sc.parallelize(1 to 5).map { x =>
  println(s"Processing $x")  // This goes to executor logs!
  x * 2
}

println("RDD created, calling count...")
// No "Processing" messages yet - map hasn't run!

val count = rdd.count()  // NOW you'll see the prints (in executor logs)
println(s"Count: $count")


// ═══════════════════════════════════════════════════════════════════════════
// PROOF 3: Using timestamps
// ═══════════════════════════════════════════════════════════════════════════

val startTransform = System.currentTimeMillis()

val rdd = sc.parallelize(1 to 1000000)
  .map(_ * 2)
  .filter(_ > 100)
  .map(_ + 1)

val endTransform = System.currentTimeMillis()
println(s"Transformations took: ${endTransform - startTransform}ms")  // ~0ms!

val startAction = System.currentTimeMillis()
val result = rdd.count()  // Now real work happens
val endAction = System.currentTimeMillis()
println(s"Action took: ${endAction - startAction}ms")  // Actual processing time
```

## What Triggers Execution?

```
ACTIONS - THE EXECUTION TRIGGERS
═══════════════════════════════════════════════════════════════════════════════

  Actions fall into three categories:

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  CATEGORY 1: Return data to driver                                          │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • collect()      → Array[T]         // All elements (dangerous for big!)  │
  │  • take(n)        → Array[T]         // First n elements                   │
  │  • first()        → T                // First element                      │
  │  • count()        → Long             // Number of elements                 │
  │  • reduce(f)      → T                // Aggregate all elements             │
  │  • fold(z)(f)     → T                // Aggregate with zero value          │
  │  • aggregate()    → U                // Aggregate with different types     │
  │  • countByKey()   → Map[K, Long]     // Count per key                      │
  │  • takeSample()   → Array[T]         // Random sample                      │
  │  • takeOrdered(n) → Array[T]         // Top/bottom n elements              │
  │                                                                             │
  │  CATEGORY 2: Write to storage                                               │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • saveAsTextFile(path)              // Write as text                      │
  │  • saveAsObjectFile(path)            // Write serialized objects           │
  │  • saveAsSequenceFile(path)          // Hadoop SequenceFile                │
  │  • saveAsHadoopFile(...)             // Custom Hadoop format               │
  │                                                                             │
  │  CATEGORY 3: Execute for side effects                                       │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  • foreach(f)            → Unit      // Apply f to each element            │
  │  • foreachPartition(f)   → Unit      // Apply f to each partition          │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘


  IMPORTANT: Calling multiple actions = multiple executions!
  ─────────────────────────────────────────────────────────────────────────────

  val processed = sc.textFile("huge.txt").map(expensive).filter(condition)

  processed.count()  // Reads file, maps, filters → count
  processed.first()  // Reads file AGAIN, maps, filters → first element

  Solution: Cache if you need multiple actions (see Kata 08)
```

## Execution Flow in Detail

```
WHAT HAPPENS WHEN YOU CALL AN ACTION
═══════════════════════════════════════════════════════════════════════════════

  Code: val result = rdd.map(f).filter(g).reduceByKey(h).count()

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │  Step 1: DRIVER - Build the DAG                                            │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  ┌────────┐    ┌────────┐    ┌────────┐    ┌────────┐    ┌────────┐       │
  │  │  RDD   │───►│  map   │───►│ filter │───►│ reduce │───►│ count  │       │
  │  └────────┘    └────────┘    └────────┘    └────────┘    └────────┘       │
  │                                                                             │
  │                                                                             │
  │  Step 2: DAGScheduler - Create Stages                                      │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  ┌─────────────────────────────┐      ┌─────────────────────────────┐      │
  │  │         Stage 0             │      │         Stage 1             │      │
  │  │  RDD → map → filter        │═════►│  reduceByKey → count        │      │
  │  │  (Narrow transformations)   │      │  (After shuffle)            │      │
  │  └─────────────────────────────┘      └─────────────────────────────┘      │
  │                                SHUFFLE                                      │
  │                                                                             │
  │  Step 3: TaskScheduler - Create Tasks                                      │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  Stage 0 (4 partitions = 4 tasks):                                         │
  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐                              │
  │  │ Task 0 │ │ Task 1 │ │ Task 2 │ │ Task 3 │                              │
  │  │ P0→map │ │ P1→map │ │ P2→map │ │ P3→map │                              │
  │  │ →filter│ │ →filter│ │ →filter│ │ →filter│                              │
  │  └────────┘ └────────┘ └────────┘ └────────┘                              │
  │                                                                             │
  │  Stage 1 (4 partitions = 4 tasks):                                         │
  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐                              │
  │  │ Task 0 │ │ Task 1 │ │ Task 2 │ │ Task 3 │                              │
  │  │ reduce │ │ reduce │ │ reduce │ │ reduce │                              │
  │  │ +count │ │ +count │ │ +count │ │ +count │                              │
  │  └────────┘ └────────┘ └────────┘ └────────┘                              │
  │                                                                             │
  │  Step 4: Execute on Cluster                                                │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │                                                                             │
  │  Executor 1          Executor 2          Executor 3                        │
  │  ┌──────────┐       ┌──────────┐       ┌──────────┐                        │
  │  │ Task 0,2 │       │ Task 1   │       │ Task 3   │                        │
  │  │          │       │          │       │          │                        │
  │  │ Results  │       │ Results  │       │ Results  │                        │
  │  └────┬─────┘       └────┬─────┘       └────┬─────┘                        │
  │       │                  │                  │                               │
  │       └──────────────────┴──────────────────┘                               │
  │                          │                                                  │
  │                          ▼                                                  │
  │                     ┌──────────┐                                            │
  │                     │  Driver  │                                            │
  │                     │  count() │                                            │
  │                     │ = 42567  │                                            │
  │                     └──────────┘                                            │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Viewing the DAG and Lineage

```scala
// ═══════════════════════════════════════════════════════════════════════════
// toDebugString - View the lineage
// ═══════════════════════════════════════════════════════════════════════════

val lines = sc.textFile("data.txt")
val words = lines.flatMap(_.split(" "))
val filtered = words.filter(_.length > 3)
val pairs = filtered.map(w => (w, 1))
val counts = pairs.reduceByKey(_ + _)

println(counts.toDebugString)

// Output:
// (4) ShuffledRDD[5] at reduceByKey at <console>:27 []
//  +-(4) MapPartitionsRDD[4] at map at <console>:26 []
//     |  MapPartitionsRDD[3] at filter at <console>:25 []
//     |  MapPartitionsRDD[2] at flatMap at <console>:24 []
//     |  MapPartitionsRDD[1] at textFile at <console>:23 []
//     |  HadoopRDD[0] at textFile at <console>:23 []

// Reading the output:
// (4) = number of partitions
// └── = narrow dependency (same stage)
// +-- = wide dependency (new stage, shuffle)


// ═══════════════════════════════════════════════════════════════════════════
// dependencies - Programmatic access
// ═══════════════════════════════════════════════════════════════════════════

println(counts.dependencies)
// List(ShuffleDependency[4, String, Int])

println(pairs.dependencies)
// List(OneToOneDependency[3])  (narrow)


// ═══════════════════════════════════════════════════════════════════════════
// Spark UI - Visual inspection
// ═══════════════════════════════════════════════════════════════════════════

// When running with spark-shell or a Spark application:
// Navigate to http://localhost:4040
//
// The DAG Visualization shows:
// • Stages as boxes
// • RDDs within stages
// • Shuffle boundaries between stages
// • Task metrics (time, data size, etc.)
```

## Lazy Evaluation Patterns

```scala
// ═══════════════════════════════════════════════════════════════════════════
// PATTERN 1: Building complex pipelines
// ═══════════════════════════════════════════════════════════════════════════

// Laziness allows building pipelines incrementally
def buildPipeline(rdd: RDD[String], config: Config): RDD[(String, Int)] = {
  var result = rdd

  // Conditionally add transformations - all lazy!
  if (config.filterEmpty) {
    result = result.filter(_.nonEmpty)
  }
  if (config.toLowerCase) {
    result = result.map(_.toLowerCase)
  }
  if (config.removeStopWords) {
    result = result.filter(w => !stopWords.contains(w))
  }

  result.map(w => (w, 1)).reduceByKey(_ + _)
}

// Nothing executes until an action is called on the result


// ═══════════════════════════════════════════════════════════════════════════
// PATTERN 2: Short-circuit evaluation (kind of)
// ═══════════════════════════════════════════════════════════════════════════

val rdd = sc.parallelize(1 to 1000000)
  .map(heavyComputation)  // Expensive!
  .filter(_ > threshold)

// first() only processes until it finds one matching element
val firstMatch = rdd.first()  // May not process all 1M elements!

// take(n) processes until it finds n elements
val topFive = rdd.take(5)  // May stop early


// ═══════════════════════════════════════════════════════════════════════════
// PATTERN 3: Debugging with laziness awareness
// ═══════════════════════════════════════════════════════════════════════════

val step1 = rdd.map(transform1)
val step2 = step1.filter(condition)
val step3 = step2.map(transform2)

// Debug by checking intermediate counts
println(s"After step1: ${step1.count()} elements")  // Triggers step1
println(s"After step2: ${step2.count()} elements")  // Triggers step1 AND step2!
println(s"After step3: ${step3.count()} elements")  // Triggers ALL steps!

// Better: cache intermediate results for debugging
step1.cache()
step2.cache()
step3.cache()

println(s"After step1: ${step1.count()} elements")  // Computes and caches step1
println(s"After step2: ${step2.count()} elements")  // Uses step1 cache
println(s"After step3: ${step3.count()} elements")  // Uses step2 cache

step1.unpersist()
step2.unpersist()
step3.unpersist()
```

## Common Pitfalls

```
PITFALL 1: Expecting immediate execution
─────────────────────────────────────────────────────────────────────────────

  // This does NOTHING!
  rdd.map(x => {
    sendToDatabase(x)  // Never executed!
    x
  })

  // Need an action:
  rdd.map(x => {
    sendToDatabase(x)
    x
  }).count()  // NOW the sends happen


PITFALL 2: Confusing driver vs executor context
─────────────────────────────────────────────────────────────────────────────

  var total = 0  // On driver

  rdd.map { x =>
    total += x  // Modifies a COPY on each executor!
    x
  }.collect()

  println(total)  // Still 0! Driver's copy unchanged.

  // Solution: Use reduce or Accumulators
  val total = rdd.reduce(_ + _)


PITFALL 3: Multiple actions = multiple computations
─────────────────────────────────────────────────────────────────────────────

  val expensive = rdd.map(veryExpensiveFunction)

  val count = expensive.count()   // Computes expensive function for ALL elements
  val sum = expensive.reduce(_ + _)  // Computes AGAIN!
  val sample = expensive.take(10)    // Computes AGAIN!

  // Solution: Cache
  expensive.cache()
  val count = expensive.count()   // Computes and caches
  val sum = expensive.reduce(_ + _)  // Uses cache
  val sample = expensive.take(10)    // Uses cache
  expensive.unpersist()


PITFALL 4: Assuming order of execution
─────────────────────────────────────────────────────────────────────────────

  var step = 0

  val rdd1 = rdd.map { x =>
    println(s"Step 1: $step")
    step = 1
    x
  }

  val rdd2 = rdd1.map { x =>
    println(s"Step 2: $step")
    step = 2
    x
  }

  // You might expect Step 1 then Step 2...
  // But Spark pipelines operations!
  // For each element: Step 1, Step 2, Step 1, Step 2, ...


PITFALL 5: Side effects in transformations for control flow
─────────────────────────────────────────────────────────────────────────────

  var foundError = false

  rdd.filter { x =>
    if (x.hasError) {
      foundError = true  // Won't work as expected!
    }
    x.isValid
  }.collect()

  if (foundError) {
    // This branch won't work reliably
  }

  // Solution: Use actions to check conditions
  val errorCount = rdd.filter(_.hasError).count()
  if (errorCount > 0) {
    // Handle errors
  }
```

## Best Practices

```
BEST PRACTICE 1: Understand your action's scope
─────────────────────────────────────────────────────────────────────────────

  // count() triggers FULL pipeline execution
  rdd.map(f).filter(g).count()

  // first() may short-circuit (not process all data)
  rdd.map(f).filter(g).first()

  // take(n) processes until n elements found
  rdd.map(f).filter(g).take(10)


BEST PRACTICE 2: Use toDebugString to understand the plan
─────────────────────────────────────────────────────────────────────────────

  // Before running, check the plan
  println(result.toDebugString)

  // Look for:
  // - Number of stages (wide deps = more stages)
  // - Partition counts
  // - Types of RDDs


BEST PRACTICE 3: Cache at strategic points
─────────────────────────────────────────────────────────────────────────────

  // Expensive computation used multiple times
  val processed = raw.map(expensive).filter(complex)
  processed.cache()

  // Multiple downstream uses
  val analysis1 = processed.map(a).reduce(...)
  val analysis2 = processed.map(b).reduce(...)
  val analysis3 = processed.map(c).reduce(...)

  processed.unpersist()


BEST PRACTICE 4: Minimize actions when exploring data
─────────────────────────────────────────────────────────────────────────────

  // BAD: Many separate actions
  println(rdd.count())
  println(rdd.first())
  println(rdd.take(5).mkString(", "))

  // BETTER: Use aggregate or combine logic
  val sample = rdd.take(100)  // One action
  println(s"Sample size: ${sample.length}")
  println(s"First: ${sample.head}")
  println(s"Top 5: ${sample.take(5).mkString(", ")}")


BEST PRACTICE 5: Profile before optimizing
─────────────────────────────────────────────────────────────────────────────

  // Use Spark UI to see actual execution
  // • Stage timeline
  // • Task distribution
  // • Shuffle read/write
  // • GC time

  // Only optimize based on real bottlenecks!
```

## Instructions

1. **Read** this README thoroughly - understand lazy evaluation deeply
2. **Open** `src/main/scala/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-04-lazy-evaluation && sbt run`
4. **Experiment**: Add println statements to prove laziness
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **Transformations are lazy** - they build a DAG, not execute
2. **Actions trigger execution** - count(), collect(), save()
3. **The DAG enables optimization** - Spark sees the whole pipeline
4. **No intermediate materialization** - data flows through in one pass
5. **Lineage = fault tolerance** - the DAG is the recovery recipe
6. **Multiple actions = multiple computations** - cache if needed
7. **toDebugString** shows the execution plan
8. **Side effects in transformations are unreliable** - use actions

## Time
~30 minutes

## Next
Continue to [kata-05-partitioning](../kata-05-partitioning/)
