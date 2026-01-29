# Kata 18: Broadcast Variables and Accumulators

## Goal
Master Spark's shared variables for efficient distributed computing.

## The Problem: Sharing Data in Distributed Systems

```
WITHOUT SHARED VARIABLES:
═══════════════════════════════════════════════════════════════════

    val lookupTable = loadLookupTable()  // 100MB on driver

    rdd.map { record =>
        lookupTable.get(record.key)  // lookupTable sent with EVERY task!
    }

    ┌────────────────────────────────────────────────────────────┐
    │                        DRIVER                              │
    │  lookupTable (100MB)                                       │
    └────────────────────────────────────────────────────────────┘
              │             │             │
              │ 100MB       │ 100MB       │ 100MB
              ▼             ▼             ▼
         ┌────────┐    ┌────────┐    ┌────────┐
         │ Task 1 │    │ Task 2 │    │ Task 3 │  ... 1000 tasks
         │ 100MB  │    │ 100MB  │    │ 100MB  │
         └────────┘    └────────┘    └────────┘

    Total data transferred: 100MB × 1000 = 100GB!
    Same data sent redundantly to every task!
```

## Broadcast Variables

```
WITH BROADCAST:
═══════════════════════════════════════════════════════════════════

    val lookupTable = loadLookupTable()  // 100MB on driver
    val broadcastTable = sc.broadcast(lookupTable)  // Broadcast once

    rdd.map { record =>
        broadcastTable.value.get(record.key)  // .value to access
    }

    ┌────────────────────────────────────────────────────────────┐
    │                        DRIVER                              │
    │  broadcastTable (100MB)                                    │
    └────────────────────────────────────────────────────────────┘
              │
              │ 100MB (sent ONCE per executor, not per task)
              ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    EXECUTOR 1                               │
    │  ┌──────────────────────────────────────────────────────┐  │
    │  │ Broadcast Block (100MB) - shared by all tasks        │  │
    │  └──────────────────────────────────────────────────────┘  │
    │     ↑          ↑          ↑          ↑                     │
    │  Task 1    Task 2    Task 3    Task 4                      │
    └─────────────────────────────────────────────────────────────┘

    With 10 executors: 100MB × 10 = 1GB (instead of 100GB!)
```

## Broadcast Internals: BitTorrent Protocol

```
HOW BROADCAST WORKS:
═══════════════════════════════════════════════════════════════════

    Phase 1: Driver splits data into blocks
    ┌─────────────────────────────────────────────────────────────┐
    │  Driver: 100MB data → [Block1][Block2][Block3][Block4]     │
    │                         25MB    25MB    25MB    25MB        │
    └─────────────────────────────────────────────────────────────┘

    Phase 2: BitTorrent-like distribution
    ┌─────────────────────────────────────────────────────────────┐
    │                                                             │
    │  Driver ──Block1──► Executor1                              │
    │         ──Block2──► Executor2                              │
    │                                                             │
    │  Then executors share with each other:                     │
    │                                                             │
    │  Executor1 ──Block1──► Executor2, Executor3                │
    │  Executor2 ──Block2──► Executor1, Executor3                │
    │  Executor3 ──Block3──► Executor1, Executor2                │
    │                                                             │
    │  Parallel distribution = O(log n) instead of O(n)!         │
    └─────────────────────────────────────────────────────────────┘
```

## When to Use Broadcast

```
USE BROADCAST WHEN:
═══════════════════════════════════════════════════════════════════

    ✓ Lookup tables / dictionaries
    ✓ ML model weights for prediction
    ✓ Configuration data
    ✓ Small dimension tables for joins
    ✓ Any read-only data > ~10KB used in multiple tasks

    Rule of thumb: If data is used by many tasks and doesn't
    fit in closure efficiently, broadcast it.


DON'T BROADCAST:
═══════════════════════════════════════════════════════════════════

    ✗ Very small data (closure is fine)
    ✗ Data that changes (broadcast is immutable)
    ✗ Very large data (won't fit in executor memory)
    ✗ Data used by only one task
```

## Accumulators: Aggregating from Workers to Driver

```
THE PROBLEM: How to count things across distributed tasks?
═══════════════════════════════════════════════════════════════════

    var errorCount = 0  // This WON'T work!

    rdd.foreach { record =>
        if (record.isError) errorCount += 1  // Updates local copy!
    }

    println(errorCount)  // Still 0! (driver's copy unchanged)


    Why? Each executor has its own copy of errorCount.
    Updates don't propagate back to driver.

    ┌──────────┐
    │  Driver  │  errorCount = 0 (unchanged!)
    └──────────┘
         │
    ┌────┴────┬────────────┐
    │         │            │
    ▼         ▼            ▼
  Exec 1    Exec 2      Exec 3
  count=5   count=3     count=7
    │         │            │
    └────┬────┴────────────┘
         │
         ✗ No way back to driver!
```

## Accumulator Solution

```
WITH ACCUMULATOR:
═══════════════════════════════════════════════════════════════════

    val errorCount = sc.longAccumulator("errorCount")

    rdd.foreach { record =>
        if (record.isError) errorCount.add(1)  // Thread-safe add
    }

    println(errorCount.value)  // Correct total!


    ┌──────────────────────────────────────────────────────────────┐
    │                          DRIVER                              │
    │  errorCount = LongAccumulator                                │
    │                                                              │
    │  After job: errorCount.value = 15                           │
    └──────────────────────────────────────────────────────────────┘
                              ▲
                              │ Aggregated on job completion
         ┌────────────────────┼────────────────────┐
         │                    │                    │
    ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
    │ Exec 1  │          │ Exec 2  │          │ Exec 3  │
    │ adds: 5 │          │ adds: 3 │          │ adds: 7 │
    └─────────┘          └─────────┘          └─────────┘
```

## Accumulator Gotchas

```
⚠️  CRITICAL: Accumulators in Transformations
═══════════════════════════════════════════════════════════════════

    val counter = sc.longAccumulator("counter")

    val rdd = sc.parallelize(1 to 100)
        .map { x =>
            counter.add(1)  // Called in TRANSFORMATION
            x * 2
        }

    rdd.count()  // Action 1
    println(counter.value)  // 100 ✓

    rdd.collect()  // Action 2 - RDD recomputed!
    println(counter.value)  // 200! (counted twice!)


WHY?
─────────────────────────────────────────────────────────────────
    Transformations are lazy. If RDD is recomputed (not cached),
    the accumulator updates happen again!


SAFE USAGE:
─────────────────────────────────────────────────────────────────
    // Use accumulators only in ACTIONS
    rdd.foreach { x =>
        counter.add(1)  // Safe: foreach is an action
    }

    // Or cache the RDD
    val cachedRdd = rdd.cache()
    cachedRdd.count()   // Accumulator updated once
    cachedRdd.collect() // Uses cache, no recomputation
```

## Custom Accumulators

```scala
// Custom accumulator for collecting unique values
class SetAccumulator extends AccumulatorV2[String, Set[String]] {
    private var _set: Set[String] = Set.empty

    override def isZero: Boolean = _set.isEmpty
    override def copy(): SetAccumulator = {
        val newAcc = new SetAccumulator
        newAcc._set = _set
        newAcc
    }
    override def reset(): Unit = _set = Set.empty
    override def add(v: String): Unit = _set = _set + v
    override def merge(other: AccumulatorV2[String, Set[String]]): Unit = {
        _set = _set ++ other.value
    }
    override def value: Set[String] = _set
}

// Usage
val uniqueErrors = new SetAccumulator
sc.register(uniqueErrors, "uniqueErrors")

rdd.foreach { record =>
    if (record.hasError) uniqueErrors.add(record.errorType)
}

println(uniqueErrors.value)  // Set("NullPointer", "OutOfBounds", ...)
```

## Broadcast Join Pattern

```scala
// Small table join using broadcast (no shuffle!)
val smallTable = spark.read.parquet("dim_products")  // 50MB
val largeTable = spark.read.parquet("fact_sales")    // 500GB

// Without broadcast: SHUFFLE join (expensive!)
val joined1 = largeTable.join(smallTable, "product_id")

// With broadcast: No shuffle!
import org.apache.spark.sql.functions.broadcast
val joined2 = largeTable.join(broadcast(smallTable), "product_id")

// Auto-broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
```

## Memory Considerations

```
BROADCAST MEMORY USAGE:
═══════════════════════════════════════════════════════════════════

    Broadcast data is stored in:
    1. Driver memory (original)
    2. Each executor's storage memory (copy)

    If broadcast = 100MB and you have 50 executors:
    - Driver: 100MB
    - Executors: 50 × 100MB = 5GB total cluster memory

    ⚠️ Don't broadcast data larger than executor memory!


CONFIGURATION:
─────────────────────────────────────────────────────────────────
    spark.broadcast.blockSize = 4m       # Block size for distribution
    spark.sql.autoBroadcastJoinThreshold = 10m  # Auto-broadcast threshold
```

## Instructions

1. **Read** this README - understand broadcast vs accumulator use cases
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-18-broadcast-accumulators && sbt run`
4. **Experiment** with different broadcast sizes
5. **Check** `solution/Solution.scala` if needed

## Quick Reference

| Feature | Broadcast | Accumulator |
|---------|-----------|-------------|
| Direction | Driver → Workers | Workers → Driver |
| Mutability | Immutable | Write-only (workers), Read (driver) |
| Use Case | Lookup tables, config | Counters, metrics |
| Memory | Stored on each executor | Aggregated on driver |

## Time
~30 minutes

## Next
Continue to [kata-19-spark-sql-internals](../kata-19-spark-sql-internals/)
