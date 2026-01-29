# Kata 02: RDD Basics - The Foundation

## Goal
Understand RDDs (Resilient Distributed Datasets), the fundamental data abstraction in Spark.

## Concepts Covered
- **RDD**: Immutable, distributed collection of elements
- **Partitions**: How data is split for parallel processing
- **Lineage**: How Spark tracks transformations for fault tolerance

## What is an RDD?

```
RDD = Resilient + Distributed + Dataset

┌─────────────────────────────────────────────────────────────────┐
│                           RDD                                   │
│                                                                 │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐   │
│  │ Partition │  │ Partition │  │ Partition │  │ Partition │   │
│  │     0     │  │     1     │  │     2     │  │     3     │   │
│  │  [1,2,3]  │  │  [4,5,6]  │  │  [7,8,9]  │  │ [10,11,12]│   │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘   │
│       │              │              │              │            │
│       ▼              ▼              ▼              ▼            │
│    Task 0         Task 1         Task 2         Task 3         │
│  (parallel)     (parallel)     (parallel)     (parallel)       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Key Properties of RDDs

| Property | Description |
|----------|-------------|
| **Immutable** | Once created, cannot be changed |
| **Distributed** | Data spread across cluster nodes |
| **Fault-tolerant** | Can be rebuilt using lineage |
| **Lazy** | Transformations don't execute until action |
| **Typed** | RDD[T] knows its element type |

## Creating RDDs

```scala
// From a collection
val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))

// With specific number of partitions
val rdd2 = sc.parallelize(1 to 100, numSlices = 4)

// From a file
val rdd3 = sc.textFile("path/to/file.txt")
```

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-02-rdd-basics && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~20 minutes

## Next
After completing this kata, proceed to `kata-03-transformations`
