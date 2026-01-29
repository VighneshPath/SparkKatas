# Kata 01: SparkSession - The Entry Point

## Goal
Master SparkSession creation, configuration, and understand its role as the unified entry point to all Spark functionality.

## What is SparkSession?

```
SPARKSESSION = YOUR GATEWAY TO SPARK
═══════════════════════════════════════════════════════════════════

  Before Spark 2.0:                    After Spark 2.0:
  ─────────────────                    ─────────────────
  SparkContext (RDDs)                  SparkSession
  SQLContext (SQL)           →         (Unified entry point)
  HiveContext (Hive)
  StreamingContext                     One object to rule them all!

  SparkSession provides access to:
  • DataFrame and Dataset APIs
  • SQL queries
  • Reading/writing data
  • Configuration
  • SparkContext (for RDD operations)
```

## Architecture Deep Dive

```
YOUR SPARK APPLICATION
═══════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────┐
│                         SparkSession                                 │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Builder Pattern:                                            │   │
│  │    SparkSession.builder()                                   │   │
│  │      .appName("MyApp")       // Name shown in Spark UI      │   │
│  │      .master("local[*]")     // Cluster manager URL         │   │
│  │      .config("key", "val")   // Configuration options       │   │
│  │      .getOrCreate()          // Get or create session       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                  │                                   │
│         ┌────────────────────────┼────────────────────────┐         │
│         ▼                        ▼                        ▼         │
│  ┌─────────────┐         ┌─────────────┐         ┌─────────────┐   │
│  │SparkContext │         │ SQLContext  │         │RuntimeConfig│   │
│  │             │         │             │         │             │   │
│  │ • RDD ops   │         │ • SQL ops   │         │ • Get/Set   │   │
│  │ • Accum.    │         │ • Catalog   │         │   configs   │   │
│  │ • Broadcast │         │ • Tables    │         │             │   │
│  └─────────────┘         └─────────────┘         └─────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        CLUSTER                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Executor   │  │   Executor   │  │   Executor   │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
```

## Master URL Options

```
DEPLOYMENT MODES
═══════════════════════════════════════════════════════════════════

┌─────────────────────┬────────────────────────────────────────────┐
│  Master URL         │  Description                               │
├─────────────────────┼────────────────────────────────────────────┤
│  local              │  Run locally with 1 thread                 │
│  local[4]           │  Run locally with 4 threads                │
│  local[*]           │  Run locally with all available cores      │
│  spark://host:7077  │  Connect to Spark Standalone cluster       │
│  yarn               │  Connect to YARN cluster                   │
│  k8s://host:port    │  Connect to Kubernetes cluster             │
│  mesos://host:5050  │  Connect to Mesos cluster                  │
└─────────────────────┴────────────────────────────────────────────┘

For local development/testing, use:  local[*]
For production, use your cluster manager (yarn, k8s, etc.)
```

## Configuration Options

```
COMMON CONFIGURATIONS
═══════════════════════════════════════════════════════════════════

Application Settings:
─────────────────────────────────────────────────────────────────
  spark.app.name              Application name
  spark.master                Cluster manager URL

Memory Settings:
─────────────────────────────────────────────────────────────────
  spark.driver.memory         Driver memory (default: 1g)
  spark.executor.memory       Executor memory (default: 1g)
  spark.memory.fraction       Fraction for Spark (default: 0.6)

Parallelism Settings:
─────────────────────────────────────────────────────────────────
  spark.default.parallelism   Default partitions for RDDs
  spark.sql.shuffle.partitions Partitions for shuffles (default: 200)

Serialization Settings:
─────────────────────────────────────────────────────────────────
  spark.serializer            Use KryoSerializer for performance
  spark.kryo.registrationRequired  Require Kryo class registration

UI Settings:
─────────────────────────────────────────────────────────────────
  spark.ui.enabled            Enable Spark UI (default: true)
  spark.ui.port               Spark UI port (default: 4040)
```

## Ways to Set Configuration

```scala
// Method 1: Builder pattern (recommended)
val spark = SparkSession.builder()
  .appName("MyApp")
  .master("local[*]")
  .config("spark.executor.memory", "2g")
  .config("spark.sql.shuffle.partitions", "100")
  .getOrCreate()

// Method 2: SparkConf object
val conf = new SparkConf()
  .setAppName("MyApp")
  .setMaster("local[*]")
  .set("spark.executor.memory", "2g")

val spark = SparkSession.builder()
  .config(conf)
  .getOrCreate()

// Method 3: Runtime configuration (some settings)
spark.conf.set("spark.sql.shuffle.partitions", "100")

// Method 4: Command line (when submitting)
// spark-submit --conf spark.executor.memory=2g ...

// Method 5: spark-defaults.conf file
// Located in $SPARK_HOME/conf/
```

## SparkSession vs SparkContext

```
WHEN TO USE WHICH?
═══════════════════════════════════════════════════════════════════

  SparkSession (spark)              SparkContext (sc)
  ─────────────────────             ─────────────────────
  • DataFrames                      • RDDs
  • Datasets                        • Accumulators
  • SQL queries                     • Broadcast variables
  • Reading files                   • Low-level operations
  • Structured APIs                 • Fine-grained control

  Access SparkContext from SparkSession:
  val sc = spark.sparkContext

  RECOMMENDATION:
  Start with SparkSession and DataFrames/Datasets.
  Use SparkContext and RDDs only when you need low-level control.
```

## Session Lifecycle

```
LIFECYCLE MANAGEMENT
═══════════════════════════════════════════════════════════════════

  1. CREATE (once per application)
     val spark = SparkSession.builder()...getOrCreate()

  2. USE (throughout application)
     val df = spark.read.json("data.json")
     df.show()

  3. STOP (when done - important!)
     spark.stop()

  ⚠️ IMPORTANT:
  • Only ONE active SparkSession per JVM (usually)
  • getOrCreate() returns existing session if one exists
  • Always call stop() to release resources
  • In notebooks/shells, session is pre-created as 'spark'
```

## Singleton Pattern

```scala
// SparkSession is typically a singleton
object SparkSessionManager {
  @transient private var spark: SparkSession = _

  def getSession: SparkSession = {
    if (spark == null) {
      spark = SparkSession.builder()
        .appName("MyApp")
        .master("local[*]")
        .getOrCreate()
    }
    spark
  }

  def stopSession(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }
}
```

## Spark UI

```
ACCESSING SPARK UI
═══════════════════════════════════════════════════════════════════

  While your application runs, visit:
  http://localhost:4040

  Tabs available:
  ┌─────────┬──────────────────────────────────────────────────────┐
  │  Jobs   │  All jobs, their stages, and status                 │
  │  Stages │  Details about each stage, tasks, shuffle           │
  │  Storage│  Cached RDDs and their memory usage                 │
  │  Environ│  Spark configuration and environment                │
  │  Execut │  Executor information, memory, tasks                │
  │  SQL    │  SQL queries and their execution plans              │
  └─────────┴──────────────────────────────────────────────────────┘

  If port 4040 is busy, Spark tries 4041, 4042, etc.
```

## Instructions

1. **Read** this README thoroughly
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-01-spark-session && sbt run`
4. **Open** http://localhost:4040 while running to see Spark UI
5. **Check** `solution/Solution.scala` if needed

## Key Takeaways

1. **SparkSession is the entry point** - Create it first, always
2. **Builder pattern** - Use builder() for configuration
3. **local[*]** - Use all cores for local development
4. **getOrCreate()** - Gets existing or creates new session
5. **spark.sparkContext** - Access SparkContext for RDD operations
6. **Always stop()** - Release resources when done
7. **Spark UI** - Monitor your application at localhost:4040

## Common Mistakes

```
❌ WRONG: Creating multiple SparkSessions
   val spark1 = SparkSession.builder()...getOrCreate()
   val spark2 = SparkSession.builder()...getOrCreate()  // Same session!

✅ RIGHT: Use getOrCreate() to get singleton

❌ WRONG: Forgetting to stop
   def main(): Unit = {
     val spark = SparkSession.builder()...getOrCreate()
     // ... do work ...
     // Oops! Forgot spark.stop()
   }

✅ RIGHT: Always stop in finally block or use try-with-resources

❌ WRONG: Hardcoding master URL in production code
   .master("local[*]")  // Don't do this for production!

✅ RIGHT: Set master via spark-submit or environment
   // Let spark-submit provide the master URL
```

## Time
~15 minutes

## Next
Continue to [kata-02-rdd-basics](../kata-02-rdd-basics/)
