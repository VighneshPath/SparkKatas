# Kata 14: Jobs, Stages, and Tasks

## Goal
Deeply understand Spark's execution hierarchy and how your code maps to physical execution.

## The Execution Hierarchy

```
YOUR CODE
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│                        APPLICATION                               │
│  One SparkContext = One Application                             │
│  Created when you call SparkSession.builder().getOrCreate()     │
└─────────────────────────────────────────────────────────────────┘
    │
    │ Each ACTION triggers a Job
    ▼
┌─────────────────────────────────────────────────────────────────┐
│                           JOB                                    │
│  Triggered by: collect(), count(), save(), reduce(), etc.       │
│  One action = One job                                           │
│                                                                  │
│  rdd.map(...).filter(...).count()  ← This creates ONE job       │
│  rdd.map(...).filter(...).collect() ← This creates ANOTHER job  │
└─────────────────────────────────────────────────────────────────┘
    │
    │ Jobs are split at SHUFFLE BOUNDARIES
    ▼
┌─────────────────────────────────────────────────────────────────┐
│                          STAGE                                   │
│  A stage contains transformations that can be PIPELINED         │
│  (executed together without shuffling data)                     │
│                                                                  │
│  Stage boundary occurs at:                                      │
│  - reduceByKey, groupByKey (shuffle)                           │
│  - join, cogroup (shuffle)                                      │
│  - repartition (shuffle)                                        │
│  - sortByKey (shuffle)                                          │
│                                                                  │
│  Within a stage: map → filter → map (all pipelined, no shuffle) │
└─────────────────────────────────────────────────────────────────┘
    │
    │ Each partition in a stage = One Task
    ▼
┌─────────────────────────────────────────────────────────────────┐
│                          TASK                                    │
│  The smallest unit of work                                      │
│  One task processes ONE partition                               │
│                                                                  │
│  If RDD has 100 partitions and 2 stages:                       │
│  Total tasks = 100 × 2 = 200 tasks                             │
│                                                                  │
│  Tasks run in parallel across executor cores                    │
└─────────────────────────────────────────────────────────────────┘
```

## Detailed Example

```scala
val words = sc.textFile("file.txt", 4)  // 4 partitions
  .flatMap(_.split(" "))                 // Stage 0: narrow
  .map(word => (word, 1))                // Stage 0: narrow
  .reduceByKey(_ + _)                    // ← SHUFFLE → Stage 1
  .filter(_._2 > 10)                     // Stage 1: narrow
  .collect()                             // ACTION → triggers Job
```

```
                    JOB 0 (triggered by collect())
    ┌──────────────────────────────────────────────────────────┐
    │                                                          │
    │  STAGE 0 (4 tasks)              STAGE 1 (4 tasks)       │
    │  ┌──────────────────┐           ┌──────────────────┐    │
    │  │ textFile         │           │ reduceByKey      │    │
    │  │ flatMap          │──SHUFFLE──│ filter           │    │
    │  │ map              │           │                  │    │
    │  └──────────────────┘           └──────────────────┘    │
    │                                                          │
    │  Task 0: partition 0            Task 0: partition 0     │
    │  Task 1: partition 1            Task 1: partition 1     │
    │  Task 2: partition 2            Task 2: partition 2     │
    │  Task 3: partition 3            Task 3: partition 3     │
    │                                                          │
    └──────────────────────────────────────────────────────────┘
```

## Why Does This Matter?

### 1. Performance Tuning
```
More partitions = More parallelism (up to cluster capacity)
                = More task overhead
                = Smaller data per task

Fewer partitions = Less parallelism
                 = Less overhead
                 = Larger data per task (potential OOM)

Rule of thumb: 2-4 partitions per CPU core
```

### 2. Debugging
```
Job failed? → Which stage failed? → Which task failed?

Spark UI shows:
- Jobs tab: All jobs, duration, status
- Stages tab: Shuffle read/write, task distribution
- Tasks tab: Individual task metrics, GC time, errors
```

### 3. Shuffle Optimization
```
Each shuffle = Stage boundary = Disk I/O + Network I/O

Minimize shuffles by:
- Using reduceByKey instead of groupByKey
- Pre-partitioning data with same partitioner
- Broadcasting small datasets in joins
```

## Task Scheduling Deep Dive

```
                         DRIVER
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼
    ┌─────────┐        ┌─────────┐        ┌─────────┐
    │Executor1│        │Executor2│        │Executor3│
    ├─────────┤        ├─────────┤        ├─────────┤
    │ Core 1  │        │ Core 1  │        │ Core 1  │
    │ [Task]  │        │ [Task]  │        │ [Task]  │
    ├─────────┤        ├─────────┤        ├─────────┤
    │ Core 2  │        │ Core 2  │        │ Core 2  │
    │ [Task]  │        │ [Task]  │        │ [Task]  │
    └─────────┘        └─────────┘        └─────────┘

    3 executors × 2 cores = 6 concurrent tasks

    If job has 18 tasks: runs in 3 waves (18/6 = 3)
```

## Task Locality Levels

```
PROCESS_LOCAL → Data on same executor (fastest)
NODE_LOCAL    → Data on same node, different executor
RACK_LOCAL    → Data on same rack
ANY           → Data anywhere (slowest)

Spark waits (spark.locality.wait) before falling back to lower locality
```

## Instructions

1. **Read** this README thoroughly
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-14-jobs-stages-tasks && sbt run`
4. **Open** Spark UI at http://localhost:4040 while running
5. **Check** `solution/Solution.scala` if needed

## Time
~30 minutes

## Next
Continue to [kata-15-dag-scheduler](../kata-15-dag-scheduler/)
