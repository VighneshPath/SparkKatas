# Kata 15: DAG Scheduler

## Goal
Understand how Spark builds and optimizes the Directed Acyclic Graph (DAG) of computations.

## What is the DAG?

```
DAG = Directed Acyclic Graph = Your computation plan

"Directed"  → Edges have direction (parent → child)
"Acyclic"   → No cycles (can't go back to where you started)
"Graph"     → Nodes (RDDs) connected by edges (transformations)
```

## How Spark Builds the DAG

```
YOUR CODE:
─────────────────────────────────────────────────────────────────
val rdd1 = sc.textFile("data.txt")      // RDD 1
val rdd2 = rdd1.flatMap(_.split(" "))   // RDD 2 depends on RDD 1
val rdd3 = rdd2.map(w => (w, 1))        // RDD 3 depends on RDD 2
val rdd4 = rdd3.reduceByKey(_ + _)      // RDD 4 depends on RDD 3
rdd4.collect()                           // ACTION!

RESULTING DAG:
─────────────────────────────────────────────────────────────────

    ┌─────────────┐
    │   textFile  │  RDD 1 (HadoopRDD)
    │  data.txt   │
    └──────┬──────┘
           │ narrow dependency
           ▼
    ┌─────────────┐
    │   flatMap   │  RDD 2 (MapPartitionsRDD)
    │ split(" ")  │
    └──────┬──────┘
           │ narrow dependency
           ▼
    ┌─────────────┐
    │     map     │  RDD 3 (MapPartitionsRDD)
    │  (w, 1)     │
    └──────┬──────┘
           │ WIDE dependency (shuffle)
           ▼
    ┌─────────────┐
    │ reduceByKey │  RDD 4 (ShuffledRDD)
    │   _ + _     │
    └─────────────┘
```

## DAG Scheduler's Job

```
WHAT DAG SCHEDULER DOES:
═══════════════════════════════════════════════════════════════

1. RECEIVES the logical DAG when an action is called

2. SPLITS the DAG into stages at shuffle boundaries
   ┌────────────────────────────────────────────────────────┐
   │  Stage 0: textFile → flatMap → map                    │
   │  ──────── SHUFFLE BOUNDARY ────────                   │
   │  Stage 1: reduceByKey                                 │
   └────────────────────────────────────────────────────────┘

3. DETERMINES optimal execution order
   - Parent stages must complete before child stages
   - Independent stages can run in parallel

4. SUBMITS stages to TaskScheduler
   - Each stage becomes a TaskSet
   - One task per partition
```

## Lineage and Fault Tolerance

```
LINEAGE = The recipe to recreate any RDD

If a partition is lost, Spark can recompute it by:
1. Looking at the DAG to find the parent RDD
2. Recomputing just that partition from the parent
3. No need to recompute the entire RDD!

Example: If partition 2 of rdd4 is lost:
─────────────────────────────────────────────────────────────────

    rdd1.partition[0,1,2,3]     Only need to trace
           │                    back partition 2's
           ▼                    lineage!
    rdd2.partition[0,1,2,3]
           │                    ┌──────────────────┐
           ▼                    │ Recompute path:  │
    rdd3.partition[0,1,2,3]     │ rdd1[2] → rdd2[2]│
           │                    │ → rdd3[2] →      │
           ▼                    │ rdd4[2]          │
    rdd4.partition[0,1,●,3]     └──────────────────┘
                    ↑
               LOST! Recompute from lineage
```

## Dependency Types

```
NARROW DEPENDENCY (One-to-One)
═══════════════════════════════════════════════════════════════
Each partition of parent → At most one partition of child
Examples: map, filter, flatMap, union

    Parent RDD                Child RDD
    ┌───┬───┬───┬───┐        ┌───┬───┬───┬───┐
    │ 0 │ 1 │ 2 │ 3 │   →    │ 0 │ 1 │ 2 │ 3 │
    └───┴───┴───┴───┘        └───┴───┴───┴───┘
      │   │   │   │            │   │   │   │
      └───┼───┼───┼────────────┘   │   │   │
          └───┼───┼────────────────┘   │   │
              └───┼────────────────────┘   │
                  └────────────────────────┘

    Can be pipelined! No shuffle needed.


WIDE DEPENDENCY (One-to-Many / Shuffle)
═══════════════════════════════════════════════════════════════
Each partition of parent → Multiple partitions of child
Examples: reduceByKey, groupByKey, join, repartition

    Parent RDD                Child RDD
    ┌───┬───┬───┬───┐        ┌───┬───┬───┬───┐
    │ 0 │ 1 │ 2 │ 3 │        │ 0 │ 1 │ 2 │ 3 │
    └───┴───┴───┴───┘        └───┴───┴───┴───┘
      │╲  │╲  │╲  │╲           ╱│  ╱│  ╱│  ╱│
      │ ╲ │ ╲ │ ╲ │ ╲         ╱ │ ╱ │ ╱ │ ╱ │
      │  ╲│  ╲│  ╲│  ╲       ╱  │╱  │╱  │╱  │
      └───┴───┴───┴───┴─────┴───┴───┴───┴───┘
                   SHUFFLE

    Requires data movement across cluster!
```

## toDebugString Deep Dive

```scala
val rdd = sc.parallelize(1 to 100, 4)
  .map(x => (x % 10, x))
  .reduceByKey(_ + _)
  .sortByKey()

println(rdd.toDebugString)
```

```
Output explained:
─────────────────────────────────────────────────────────────────

(4) ShuffledRDD[5] at sortByKey            ← STAGE 2: 4 partitions
 +-(4) ShuffledRDD[3] at reduceByKey       ← STAGE 1: 4 partitions
    +-(4) MapPartitionsRDD[2] at map       ← STAGE 0
       +-(4) ParallelCollectionRDD[0]      ← STAGE 0

READING THE OUTPUT:
- (4) = number of partitions
- +- = dependency
- Indentation shows parent-child relationships
- ShuffledRDD = stage boundary
```

## DAG Visualization in Spark UI

```
SPARK UI → Jobs Tab → Click on Job → DAG Visualization

Shows:
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│    ┌─────────┐     ┌─────────┐     ┌─────────┐            │
│    │ Stage 0 │────▶│ Stage 1 │────▶│ Stage 2 │            │
│    │ 4 tasks │     │ 4 tasks │     │ 4 tasks │            │
│    └─────────┘     └─────────┘     └─────────┘            │
│                                                             │
│    Blue = Completed                                        │
│    Green = Running                                         │
│    Red = Failed                                            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Optimization: Breaking Long Lineages

```
PROBLEM: Very long lineage = expensive recomputation on failure

val rdd = sc.textFile("huge.txt")
  .map(...)
  .filter(...)
  .map(...)
  // ... 50 more transformations ...
  .reduceByKey(...)

If a partition fails near the end, Spark must recompute
the ENTIRE lineage from the beginning!

SOLUTION: Checkpoint or cache at strategic points

rdd.checkpoint()  // Saves to reliable storage, truncates lineage
// or
rdd.cache()       // Keeps in memory, but lineage still exists
```

## Instructions

1. **Read** this README thoroughly - understand the diagrams
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-15-dag-scheduler && sbt run`
4. **Examine** the `toDebugString` output carefully
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **DAG = Your computation plan** - Spark builds it lazily
2. **Stages are split at shuffles** - Wide dependencies create boundaries
3. **Lineage enables fault tolerance** - Can recompute lost partitions
4. **Narrow deps are cheap** - Can be pipelined together
5. **Wide deps are expensive** - Require shuffle (network + disk)

## Time
~30 minutes

## Next
Continue to [kata-16-task-scheduler](../kata-16-task-scheduler/)
