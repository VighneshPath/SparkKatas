# Kata 03: Transformations vs Actions

## Goal
Master the two fundamental types of RDD operations and understand when computation actually happens in Spark.

## The Core Concept

```
TRANSFORMATIONS vs ACTIONS - THE FUNDAMENTAL DISTINCTION
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                             │
  │   TRANSFORMATIONS                         ACTIONS                          │
  │   ───────────────                         ───────                          │
  │                                                                             │
  │   • Create a NEW RDD from existing        • Return a result to driver      │
  │   • LAZY - nothing executes               • EAGER - triggers execution     │
  │   • Build the computation plan            • Execute the plan               │
  │   • Return: RDD[T]                        • Return: T (value/Unit)         │
  │                                                                             │
  │   Examples:                               Examples:                         │
  │   map, filter, flatMap                    collect, count, reduce           │
  │   groupByKey, reduceByKey                 take, first, foreach             │
  │   join, union, distinct                   saveAsTextFile, show             │
  │                                                                             │
  │   ANALOGY:                                ANALOGY:                          │
  │   Writing a recipe                        Cooking the meal                  │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
```

## Lazy Evaluation Visualized

```
HOW LAZY EVALUATION WORKS
═══════════════════════════════════════════════════════════════════════════════

  val lines = sc.textFile("data.txt")     // Nothing happens
  val words = lines.flatMap(_.split(" ")) // Still nothing
  val filtered = words.filter(_.length > 3)// Still nothing
  val result = filtered.count()            // NOW everything executes!


  TIMELINE:
  ─────────────────────────────────────────────────────────────────────────────

  Time ──────────────────────────────────────────────────────────────────────►

  textFile()    flatMap()     filter()      count()
      │             │             │             │
      ▼             ▼             ▼             ▼
  ┌───────┐    ┌───────┐    ┌───────┐    ┌───────────────────────────────────┐
  │ Build │    │ Build │    │ Build │    │ EXECUTE EVERYTHING!              │
  │ plan  │    │ plan  │    │ plan  │    │                                   │
  │ only  │    │ only  │    │ only  │    │ 1. Read file                     │
  │       │    │       │    │       │    │ 2. Split into words              │
  └───────┘    └───────┘    └───────┘    │ 3. Filter by length              │
       │            │            │       │ 4. Count and return              │
       │            │            │       │                                   │
       └────────────┴────────────┴──────►│ Result: 12345                    │
              No data movement!          └───────────────────────────────────┘
              No computation!                   Actual work happens here!


WHY LAZY EVALUATION?
─────────────────────────────────────────────────────────────────────────────

  1. OPTIMIZATION: Spark can analyze the entire pipeline before executing
     - Combine operations (pipelining)
     - Eliminate unnecessary work
     - Choose optimal execution strategy

  2. EFFICIENCY: Avoids materializing intermediate results
     - No temporary storage needed
     - Single pass through data when possible

  3. FAULT TOLERANCE: Full lineage available for recovery
     - Can recompute from any point if failure occurs
```

## Transformations Deep Dive

```
TRANSFORMATION CATEGORIES
═══════════════════════════════════════════════════════════════════════════════

  NARROW TRANSFORMATIONS (no shuffle)
  ─────────────────────────────────────────────────────────────────────────────
  Each output partition depends on ONE input partition.
  Can be pipelined together efficiently.

  ┌────────────┬─────────────────────────────────────────────────────────────┐
  │ map        │ Apply function to each element                              │
  │            │ rdd.map(x => x * 2)                                         │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ filter     │ Keep elements matching predicate                            │
  │            │ rdd.filter(_ > 10)                                          │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ flatMap    │ Map + flatten (one-to-many transformation)                  │
  │            │ rdd.flatMap(line => line.split(" "))                        │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ mapPartitions│ Apply function to entire partition                        │
  │            │ rdd.mapPartitions(iter => iter.map(_ * 2))                  │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ union      │ Combine two RDDs                                            │
  │            │ rdd1.union(rdd2)                                            │
  └────────────┴─────────────────────────────────────────────────────────────┘


  WIDE TRANSFORMATIONS (require shuffle)
  ─────────────────────────────────────────────────────────────────────────────
  Output partitions depend on MULTIPLE input partitions.
  Require data movement across the network (expensive!)

  ┌────────────┬─────────────────────────────────────────────────────────────┐
  │ groupByKey │ Group values by key (use reduceByKey instead if possible!)  │
  │            │ rdd.groupByKey()                                            │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ reduceByKey│ Aggregate values by key (more efficient than groupByKey)    │
  │            │ rdd.reduceByKey(_ + _)                                      │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ sortByKey  │ Sort RDD by key                                             │
  │            │ rdd.sortByKey()                                             │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ join       │ Join two RDDs by key                                        │
  │            │ rdd1.join(rdd2)                                             │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ distinct   │ Remove duplicates (requires shuffle to compare)             │
  │            │ rdd.distinct()                                              │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ repartition│ Change partition count                                      │
  │            │ rdd.repartition(10)                                         │
  └────────────┴─────────────────────────────────────────────────────────────┘
```

## Common Transformations Explained

```scala
// ═══════════════════════════════════════════════════════════════════════════
// map - Transform each element
// ═══════════════════════════════════════════════════════════════════════════

val numbers = sc.parallelize(1 to 5)
val doubled = numbers.map(_ * 2)
// Result: [2, 4, 6, 8, 10]

// Input:  [1] [2] [3] [4] [5]
//          ↓   ↓   ↓   ↓   ↓   (apply x => x * 2)
// Output: [2] [4] [6] [8] [10]


// ═══════════════════════════════════════════════════════════════════════════
// filter - Keep elements matching condition
// ═══════════════════════════════════════════════════════════════════════════

val evens = numbers.filter(_ % 2 == 0)
// Result: [2, 4]

// Input:  [1] [2] [3] [4] [5]
//          ✗   ✓   ✗   ✓   ✗   (keep if x % 2 == 0)
// Output:     [2]     [4]


// ═══════════════════════════════════════════════════════════════════════════
// flatMap - Map + Flatten (one-to-many)
// ═══════════════════════════════════════════════════════════════════════════

val lines = sc.parallelize(Seq("hello world", "spark is great"))
val words = lines.flatMap(_.split(" "))
// Result: ["hello", "world", "spark", "is", "great"]

// Input:  ["hello world"]          ["spark is great"]
//              ↓                          ↓
// Split:  ["hello", "world"]       ["spark", "is", "great"]
//              ↓                          ↓
// Flatten: "hello" "world"         "spark" "is" "great"


// ═══════════════════════════════════════════════════════════════════════════
// reduceByKey - Aggregate by key (preferred over groupByKey)
// ═══════════════════════════════════════════════════════════════════════════

val pairs = sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3), ("b", 4)))
val sums = pairs.reduceByKey(_ + _)
// Result: [("a", 4), ("b", 6)]

// Why prefer reduceByKey over groupByKey?
// reduceByKey: Combines locally BEFORE shuffle (less data transferred)
// groupByKey: Shuffles ALL data, then combines (more expensive)


// ═══════════════════════════════════════════════════════════════════════════
// mapPartitions - Process entire partition at once
// ═══════════════════════════════════════════════════════════════════════════

// Useful when you have expensive setup (DB connection, model loading)
val result = rdd.mapPartitions { iterator =>
  val connection = openDatabaseConnection()  // Once per partition
  val results = iterator.map(record => queryDatabase(connection, record))
  connection.close()
  results
}


// ═══════════════════════════════════════════════════════════════════════════
// distinct - Remove duplicates
// ═══════════════════════════════════════════════════════════════════════════

val withDupes = sc.parallelize(Seq(1, 2, 2, 3, 3, 3))
val unique = withDupes.distinct()
// Result: [1, 2, 3]
// Note: Requires shuffle to compare elements across partitions!
```

## Actions Deep Dive

```
ACTIONS - WHAT TRIGGERS EXECUTION
═══════════════════════════════════════════════════════════════════════════════

  Actions fall into three categories:

  1. RETURN TO DRIVER
  ─────────────────────────────────────────────────────────────────────────────
  ┌────────────┬─────────────────────────────────────────────────────────────┐
  │ collect()  │ Return all elements as array (DANGER: can OOM driver!)     │
  │            │ val all = rdd.collect()                                     │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ take(n)    │ Return first n elements                                     │
  │            │ val first10 = rdd.take(10)                                  │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ first()    │ Return first element (same as take(1)(0))                   │
  │            │ val one = rdd.first()                                       │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ count()    │ Return count of elements                                    │
  │            │ val n = rdd.count()                                         │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ reduce()   │ Aggregate all elements using function                       │
  │            │ val sum = rdd.reduce(_ + _)                                 │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ aggregate()│ Aggregate with initial value and combine function           │
  │            │ val result = rdd.aggregate(0)(_ + _, _ + _)                 │
  └────────────┴─────────────────────────────────────────────────────────────┘


  2. WRITE TO STORAGE
  ─────────────────────────────────────────────────────────────────────────────
  ┌──────────────────┬─────────────────────────────────────────────────────────┐
  │ saveAsTextFile   │ Write as text files                                    │
  │                  │ rdd.saveAsTextFile("hdfs:///output")                   │
  ├──────────────────┼─────────────────────────────────────────────────────────┤
  │ saveAsObjectFile │ Write as serialized objects                            │
  │                  │ rdd.saveAsObjectFile("output")                         │
  ├──────────────────┼─────────────────────────────────────────────────────────┤
  │ saveAsSequenceFile│ Write as Hadoop SequenceFile                          │
  │                  │ rdd.saveAsSequenceFile("output")                       │
  └──────────────────┴─────────────────────────────────────────────────────────┘


  3. EXECUTE FOR SIDE EFFECTS
  ─────────────────────────────────────────────────────────────────────────────
  ┌────────────┬─────────────────────────────────────────────────────────────┐
  │ foreach    │ Apply function to each element (for side effects)          │
  │            │ rdd.foreach(x => sendToKafka(x))                            │
  ├────────────┼─────────────────────────────────────────────────────────────┤
  │ foreachPartition│ Apply function to each partition                       │
  │            │ rdd.foreachPartition(iter => writeToDB(iter))               │
  └────────────┴─────────────────────────────────────────────────────────────┘
```

## Common Actions Explained

```scala
// ═══════════════════════════════════════════════════════════════════════════
// collect() - Get all elements (be careful!)
// ═══════════════════════════════════════════════════════════════════════════

val small = sc.parallelize(1 to 10)
val all = small.collect()  // Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

// WARNING: This pulls ALL data to driver!
// Safe for small RDDs, dangerous for large ones
val dangerous = bigRdd.collect()  // Can cause OutOfMemoryError!


// ═══════════════════════════════════════════════════════════════════════════
// count() - Count elements
// ═══════════════════════════════════════════════════════════════════════════

val n = rdd.count()
// Triggers full execution of the pipeline to count elements


// ═══════════════════════════════════════════════════════════════════════════
// take(n) - Get first n elements (safer than collect)
// ═══════════════════════════════════════════════════════════════════════════

val firstFive = rdd.take(5)
// Only processes enough partitions to get 5 elements
// Much safer than collect() for large RDDs


// ═══════════════════════════════════════════════════════════════════════════
// reduce() - Aggregate all elements
// ═══════════════════════════════════════════════════════════════════════════

val sum = sc.parallelize(1 to 100).reduce(_ + _)
// Result: 5050

val max = sc.parallelize(1 to 100).reduce((a, b) => if (a > b) a else b)
// Result: 100

// Note: Function must be associative and commutative!
// (a + b) + c == a + (b + c)  ✓
// a + b == b + a              ✓


// ═══════════════════════════════════════════════════════════════════════════
// foreach() - Execute for side effects
// ═══════════════════════════════════════════════════════════════════════════

// Print each element (runs on executors, not driver!)
rdd.foreach(println)  // Output goes to executor logs

// Common use: write to external systems
rdd.foreachPartition { partition =>
  val connection = openConnection()
  partition.foreach(record => connection.write(record))
  connection.close()
}


// ═══════════════════════════════════════════════════════════════════════════
// countByKey() - Count elements per key
// ═══════════════════════════════════════════════════════════════════════════

val pairs = sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3)))
val counts = pairs.countByKey()
// Result: Map("a" -> 2, "b" -> 1)


// ═══════════════════════════════════════════════════════════════════════════
// takeSample() - Random sample
// ═══════════════════════════════════════════════════════════════════════════

val sample = rdd.takeSample(withReplacement = false, num = 10)
// Returns exactly 10 random elements
```

## Understanding Execution Flow

```
WHAT HAPPENS WHEN YOU CALL AN ACTION
═══════════════════════════════════════════════════════════════════════════════

  Code:
  val lines = sc.textFile("data.txt")
  val words = lines.flatMap(_.split(" "))
  val filtered = words.filter(_.length > 3)
  val result = filtered.count()   // <-- Action!

  Step 1: Build the DAG
  ─────────────────────────────────────────────────────────────────────────────
  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │   textFile   │───►│   flatMap    │───►│   filter     │
  │  (HadoopRDD) │    │              │    │              │
  └──────────────┘    └──────────────┘    └──────────────┘

  Step 2: DAG Scheduler creates stages
  ─────────────────────────────────────────────────────────────────────────────
  Since all operations are narrow, they go in ONE stage:

  Stage 0: [textFile → flatMap → filter → count]

  Step 3: Task Scheduler creates tasks
  ─────────────────────────────────────────────────────────────────────────────
  One task per partition:

  Task 0: Partition 0 → textFile → flatMap → filter → partial count
  Task 1: Partition 1 → textFile → flatMap → filter → partial count
  Task 2: Partition 2 → textFile → flatMap → filter → partial count
  ...

  Step 4: Execute and combine results
  ─────────────────────────────────────────────────────────────────────────────
  Each task returns its partial count, driver sums them up.

  Partial counts: [1234, 5678, 9012, ...]
  Final result: 15924


VIEWING THE EXECUTION PLAN
─────────────────────────────────────────────────────────────────────────────

  // toDebugString shows the plan
  println(filtered.toDebugString)

  // Output:
  (4) MapPartitionsRDD[3] at filter at <console>:25
   |  MapPartitionsRDD[2] at flatMap at <console>:24
   |  MapPartitionsRDD[1] at textFile at <console>:23
   |  HadoopRDD[0] at textFile at <console>:23

  The (4) indicates 4 partitions
```

## Transformation Chaining Best Practices

```scala
// ═══════════════════════════════════════════════════════════════════════════
// GOOD: Chain transformations fluently
// ═══════════════════════════════════════════════════════════════════════════

val result = sc.textFile("logs.txt")
  .filter(_.contains("ERROR"))
  .map(_.split("\t"))
  .map(parts => (parts(0), parts(2)))
  .reduceByKey(_ + _)
  .sortByKey()
  .collect()


// ═══════════════════════════════════════════════════════════════════════════
// GOOD: Use descriptive intermediate variables when debugging
// ═══════════════════════════════════════════════════════════════════════════

val lines = sc.textFile("logs.txt")
val errors = lines.filter(_.contains("ERROR"))
val parsed = errors.map(_.split("\t"))
val pairs = parsed.map(parts => (parts(0), parts(2)))
val counts = pairs.reduceByKey(_ + _)
val sorted = counts.sortByKey()

// Debug intermediate steps
println(s"Errors: ${errors.count()}")
println(s"Sample: ${pairs.take(5).mkString(", ")}")


// ═══════════════════════════════════════════════════════════════════════════
// BAD: Multiple actions on same lineage (recomputes!)
// ═══════════════════════════════════════════════════════════════════════════

val processed = sc.textFile("huge.txt").map(expensive).filter(condition)

// Each action recomputes from scratch!
val count = processed.count()      // Reads file, maps, filters
val first = processed.first()      // Reads file AGAIN, maps, filters
val sample = processed.take(10)    // Reads file AGAIN!


// ═══════════════════════════════════════════════════════════════════════════
// GOOD: Cache if you'll reuse
// ═══════════════════════════════════════════════════════════════════════════

val processed = sc.textFile("huge.txt").map(expensive).filter(condition)
processed.cache()  // Or persist()

val count = processed.count()      // Reads, processes, caches
val first = processed.first()      // Uses cache!
val sample = processed.take(10)    // Uses cache!

processed.unpersist()  // Free memory when done
```

## Common Mistakes

```
MISTAKE 1: Expecting immediate execution
─────────────────────────────────────────────────────────────────────────────
  // This does NOT print anything!
  rdd.map { x =>
    println(s"Processing $x")  // Never executes!
    x * 2
  }

  // Need an action to trigger:
  rdd.map { x =>
    println(s"Processing $x")
    x * 2
  }.collect()  // NOW the prints happen (on executors)


MISTAKE 2: Using collect() on large RDDs
─────────────────────────────────────────────────────────────────────────────
  // Dangerous - can crash driver with OOM
  val allData = hugeRdd.collect()

  // Better alternatives:
  val sample = hugeRdd.take(1000)
  val sample2 = hugeRdd.sample(false, 0.001).collect()
  hugeRdd.saveAsTextFile("output/")  // Write to storage instead


MISTAKE 3: Side effects in transformations
─────────────────────────────────────────────────────────────────────────────
  var counter = 0  // On driver

  rdd.map { x =>
    counter += 1  // This modifies a COPY on each executor!
    x * 2
  }.collect()

  println(counter)  // Still 0! The driver's copy was never modified.

  // Use Accumulators for this pattern (see kata-18)


MISTAKE 4: groupByKey when reduceByKey works
─────────────────────────────────────────────────────────────────────────────
  // Inefficient - shuffles all values
  val sums = pairs.groupByKey().mapValues(_.sum)

  // Better - combines locally before shuffle
  val sums = pairs.reduceByKey(_ + _)
```

## Instructions

1. **Read** this README thoroughly - understand lazy evaluation
2. **Open** `src/main/scala/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-03-transformations && sbt run`
4. **Experiment**: Add println in transformations to verify laziness
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **Transformations** = lazy, return new RDD, build the plan
2. **Actions** = eager, trigger execution, return result
3. **Lazy evaluation** enables optimization and fault tolerance
4. **Narrow transformations** (map, filter) can be pipelined
5. **Wide transformations** (reduceByKey, join) require shuffle
6. **collect()** is dangerous on large RDDs - use take() or sample()
7. **Cache** if you'll reuse an RDD multiple times
8. **toDebugString** shows the execution plan

## Time
~25 minutes

## Next
Continue to [kata-04-lazy-evaluation](../kata-04-lazy-evaluation/)
