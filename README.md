# Spark Katas 🔥

Master Apache Spark internals through 22 hands-on exercises. Each kata is self-contained with in-depth theory, diagrams, exercises, and solutions.

## Prerequisites

- Java 11+ (Java 17 recommended)
- sbt (`brew install sbt` on macOS)
- Basic Scala knowledge

## Quick Start

```bash
# Start with Kata 01
cd kata-01-spark-session
cat README.md    # Read the theory first!
sbt run          # Then run the exercises
```

## Structure

Each kata is completely self-contained:

```
kata-XX-topic/
├── README.md          # In-depth theory, diagrams, concepts
├── build.sbt          # Dependencies (run independently)
├── src/
│   └── Exercise.scala # Exercises with TODOs (???)
└── solution/
    └── Solution.scala # Complete working solution
```

## Learning Path (22 Katas)

### Level 1: Fundamentals (Katas 1-5)
Build your foundation with core Spark concepts.

| Kata | Topic | Time | Key Concepts |
|------|-------|------|--------------|
| [01](kata-01-spark-session/) | SparkSession | 15 min | Entry point, SparkContext, configuration |
| [02](kata-02-rdd-basics/) | RDD Basics | 20 min | Resilient Distributed Datasets, partitions, lineage |
| [03](kata-03-transformations/) | Transformations | 25 min | map, filter, flatMap, reduce, reduceByKey |
| [04](kata-04-lazy-evaluation/) | Lazy Evaluation | 20 min | Transformations vs actions, execution timing |
| [05](kata-05-partitioning/) | Partitioning | 25 min | Hash/Range partitioners, repartition, coalesce |

### Level 2: Execution Model (Katas 6-10)
Understand how Spark executes your code.

| Kata | Topic | Time | Key Concepts |
|------|-------|------|--------------|
| [06](kata-06-shuffling/) | Shuffling | 25 min | Shuffle mechanics, reduceByKey vs groupByKey |
| [07](kata-07-narrow-wide-transformations/) | Narrow/Wide | 25 min | Dependency types, pipelining, stage boundaries |
| [08](kata-08-caching-persistence/) | Caching | 25 min | Storage levels, cache vs persist, unpersist |
| [09](kata-09-dataframes/) | DataFrames | 30 min | Schema, operations, SQL interop |
| [10](kata-10-catalyst-optimizer/) | Catalyst | 30 min | Query optimization, explain plans |

### Level 3: Deep Internals (Katas 11-15)
Dive into Spark's execution engine.

| Kata | Topic | Time | Key Concepts |
|------|-------|------|--------------|
| [11](kata-11-tungsten-engine/) | Tungsten | 25 min | Code generation, UnsafeRow, binary format |
| [12](kata-12-joins-broadcast/) | Joins | 25 min | Join strategies, broadcast joins, cogroup |
| [13](kata-13-memory-management/) | Memory | 20 min | Unified memory, storage vs execution |
| [14](kata-14-jobs-stages-tasks/) | Jobs/Stages/Tasks | 30 min | Execution hierarchy, Spark UI |
| [15](kata-15-dag-scheduler/) | DAG Scheduler | 30 min | DAG construction, lineage, fault tolerance |

### Level 4: Production Mastery (Katas 16-20)
Master production-ready Spark optimization.

| Kata | Topic | Time | Key Concepts |
|------|-------|------|--------------|
| [16](kata-16-task-scheduler/) | Task Scheduler | 30 min | Locality, speculation, FIFO/FAIR |
| [17](kata-17-serialization/) | Serialization | 30 min | Java vs Kryo, closure capture, NotSerializable |
| [18](kata-18-broadcast-accumulators/) | Broadcast/Accumulators | 30 min | Shared variables, counters, gotchas |
| [19](kata-19-spark-sql-internals/) | SQL Internals | 35 min | Query pipeline, AQE, CBO |
| [20](kata-20-performance-tuning/) | Performance | 40 min | Tuning checklist, skew, optimization |

### Level 5: Production Operations (Katas 21-22)
Deploy, monitor, and debug Spark in production environments.

| Kata | Topic | Time | Key Concepts |
|------|-------|------|--------------|
| [21](kata-21-cluster-deployment/) | Cluster Deployment | 25 min | spark-submit, YARN, Kubernetes, Standalone, deploy modes |
| [22](kata-22-history-server-debugging/) | History Server & Debugging | 35 min | History Server, debugging, optimization, monitoring |

**Total estimated time: ~10.5 hours**

## How to Use Each Kata

### Step 1: Read the README (Important!)
```bash
cd kata-01-spark-session
cat README.md    # Contains theory, diagrams, explanations
```

### Step 2: Complete the Exercises
Open `src/Exercise.scala` and fill in the `???` TODOs.

### Step 3: Run Your Solution
```bash
sbt run
```

### Step 4: Check the Solution (if stuck)
```bash
cat solution/Solution.scala
```

### Step 5: Move to Next Kata
```bash
cd ../kata-02-rdd-basics
```

## Architecture Overview

```
YOUR SPARK APPLICATION
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────┐
│                          DRIVER                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ SparkSession → DAG Scheduler → Task Scheduler           │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐
│     EXECUTOR 1    │ │     EXECUTOR 2    │ │     EXECUTOR 3    │
│  ┌─────┐ ┌─────┐  │ │  ┌─────┐ ┌─────┐  │ │  ┌─────┐ ┌─────┐  │
│  │Task │ │Task │  │ │  │Task │ │Task │  │ │  │Task │ │Task │  │
│  └─────┘ └─────┘  │ │  └─────┘ └─────┘  │ │  └─────┘ └─────┘  │
│  ┌─────────────┐  │ │  ┌─────────────┐  │ │  ┌─────────────┐  │
│  │    Cache    │  │ │  │    Cache    │  │ │  │    Cache    │  │
│  └─────────────┘  │ │  └─────────────┘  │ │  └─────────────┘  │
└───────────────────┘ └───────────────────┘ └───────────────────┘
```

## Key Concepts Quick Reference

### Transformations vs Actions

```
TRANSFORMATIONS (Lazy)          ACTIONS (Trigger Execution)
─────────────────────────       ───────────────────────────
map, flatMap, filter            collect, count, first
reduceByKey, groupByKey         reduce, take, foreach
join, union, distinct           saveAsTextFile, show
sortByKey, repartition
```

### Memory Configuration

```
spark.executor.memory = 8g
├── Reserved: 300MB
└── Usable: 7.7GB
    ├── Spark Memory (60%): 4.6GB
    │   ├── Storage (50%): cached RDDs, broadcast
    │   └── Execution (50%): shuffles, joins, sorts
    └── User Memory (40%): your objects, UDFs
```

### RDD vs DataFrame vs Dataset

| Feature | RDD | DataFrame | Dataset |
|---------|-----|-----------|---------|
| Type Safety | Compile-time | Runtime | Compile-time |
| Optimization | Manual | Catalyst | Catalyst |
| Serialization | Java/Kryo | Tungsten | Tungsten |
| Use Case | Low-level control | SQL/Analytics | Type-safe ETL |

## Troubleshooting

### Java Version Issues
```bash
java -version   # Should be 11+
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### Memory Errors
Increase memory in kata's build.sbt:
```scala
javaOptions ++= Seq("-Xms1G", "-Xmx4G")
```

### Slow First Run
First `sbt run` downloads dependencies. Subsequent runs are fast.

## Tips for Success

1. **Read READMEs thoroughly** - They contain essential theory
2. **Use Spark UI** - http://localhost:4040 shows execution details
3. **Print lineage** - `rdd.toDebugString` visualizes the DAG
4. **Experiment** - Modify exercises and observe behavior
5. **Don't skip katas** - Each builds on previous knowledge

## Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark UI Guide](https://spark.apache.org/docs/latest/web-ui.html)
- [Performance Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)

---

Happy learning! 🚀
