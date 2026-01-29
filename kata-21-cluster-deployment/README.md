# Kata 21: Cluster Deployment

## Goal
Master deploying Spark applications to production clusters using YARN, Kubernetes, and spark-submit.

## Deployment Overview

```
LOCAL DEVELOPMENT vs PRODUCTION CLUSTER
═══════════════════════════════════════════════════════════════════════════════

  LOCAL (what we've been doing)          PRODUCTION CLUSTER
  ─────────────────────────────          ─────────────────────
  .master("local[*]")                    spark-submit --master yarn ...

  ┌─────────────────────┐                ┌─────────────────────────────────┐
  │   Your Laptop       │                │       YARN / K8s Cluster        │
  │  ┌───────────────┐  │                │                                 │
  │  │ Driver+Exec   │  │                │  ┌──────────┐  ┌──────────┐   │
  │  │ (same JVM)    │  │                │  │ Driver   │  │ Executor │   │
  │  └───────────────┘  │                │  └──────────┘  └──────────┘   │
  │                     │                │                 ┌──────────┐   │
  │  Good for: testing  │                │                 │ Executor │   │
  └─────────────────────┘                │                 └──────────┘   │
                                         │  Good for: real workloads      │
                                         └─────────────────────────────────┘
```

## spark-submit Command

```bash
spark-submit \
  --class com.mycompany.MyApp \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4g \
  --num-executors 10 \
  --executor-cores 2 \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=200 \
  /path/to/my-app.jar \
  arg1 arg2
```

### spark-submit Options Explained

```
ESSENTIAL OPTIONS
═══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────┬────────────────────────────────────────────────────┐
│  Option                 │  Description                                       │
├─────────────────────────┼────────────────────────────────────────────────────┤
│  --class                │  Main class to run (e.g., com.company.Main)       │
│  --master               │  Cluster manager URL (yarn, k8s://, spark://)     │
│  --deploy-mode          │  client (driver local) or cluster (driver remote) │
│  --name                 │  Application name shown in UI                      │
├─────────────────────────┼────────────────────────────────────────────────────┤
│  RESOURCE OPTIONS       │                                                    │
├─────────────────────────┼────────────────────────────────────────────────────┤
│  --driver-memory        │  Memory for driver (default: 1g)                  │
│  --driver-cores         │  Cores for driver (cluster mode only)             │
│  --executor-memory      │  Memory per executor                               │
│  --executor-cores       │  Cores per executor                                │
│  --num-executors        │  Number of executors to launch                    │
│  --total-executor-cores │  Total cores across all executors (Standalone)    │
├─────────────────────────┼────────────────────────────────────────────────────┤
│  ADDITIONAL OPTIONS     │                                                    │
├─────────────────────────┼────────────────────────────────────────────────────┤
│  --conf KEY=VALUE       │  Arbitrary Spark configuration                    │
│  --jars                 │  Additional jars to include                       │
│  --files                │  Files to distribute to executors                 │
│  --py-files             │  Python files for PySpark                         │
│  --packages             │  Maven coordinates for dependencies               │
└─────────────────────────┴────────────────────────────────────────────────────┘
```

## Deploy Modes: Client vs Cluster

```
CLIENT MODE (--deploy-mode client)
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  YOUR MACHINE (Edge Node)                                                   │
  │  ┌───────────────────────────────────────┐                                 │
  │  │              DRIVER                    │ ◄── Runs locally               │
  │  │  • Receives application code          │                                 │
  │  │  • Creates SparkContext               │                                 │
  │  │  • Schedules tasks                    │                                 │
  │  │  • Collects results                   │                                 │
  │  └───────────────────────────────────────┘                                 │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Communicates with
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                           YARN CLUSTER                                      │
  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
  │  │   Executor 1   │  │   Executor 2   │  │   Executor 3   │                │
  │  │   (Container)  │  │   (Container)  │  │   (Container)  │                │
  └────────────────┘  └────────────────┘  └────────────────┘                │
  └─────────────────────────────────────────────────────────────────────────────┘

  USE WHEN:
  ✓ Interactive development (spark-shell, pyspark)
  ✓ Debugging (driver logs appear locally)
  ✓ You need to see output immediately

  PROBLEMS:
  ✗ Your machine must stay connected
  ✗ Network latency between driver and cluster
  ✗ If you close terminal, job dies


CLUSTER MODE (--deploy-mode cluster)
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  YOUR MACHINE (Edge Node)                                                   │
  │  ┌───────────────────────────────────────┐                                 │
  │  │  spark-submit                         │                                 │
  │  │  (submits job, then exits)            │ ◄── You can disconnect!        │
  │  └───────────────────────────────────────┘                                 │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Submits to
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                           YARN CLUSTER                                      │
  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
  │  │    DRIVER      │  │   Executor 1   │  │   Executor 2   │                │
  │  │  (Container)   │  │   (Container)  │  │   (Container)  │                │
  │  │  • Runs inside │  │                │  │                │                │
  │  │    cluster     │  │                │  │                │                │
  └────────────────┘  └────────────────┘  └────────────────┘                │
  └─────────────────────────────────────────────────────────────────────────────┘

  USE WHEN:
  ✓ Production jobs
  ✓ Scheduled/automated jobs
  ✓ Long-running jobs
  ✓ You want to disconnect after submitting

  PROBLEMS:
  ✗ Harder to debug (logs in cluster)
  ✗ Can't use interactive shells
```

## YARN Configuration

```
YARN-SPECIFIC OPTIONS
═══════════════════════════════════════════════════════════════════════════════

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue production \                    # YARN queue
  --num-executors 20 \
  --executor-memory 8g \
  --executor-cores 4 \
  --driver-memory 4g \
  --conf spark.yarn.maxAppAttempts=2 \    # Retry failed apps
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.yarn.am.memory=1g \        # Application Master memory
  myapp.jar


YARN QUEUES
─────────────────────────────────────────────────────────────────────────────
  Queues control resource allocation and priority:

  --queue production    # High priority, more resources
  --queue development   # Lower priority
  --queue default       # Default queue

  Configure in YARN's capacity-scheduler.xml or fair-scheduler.xml


DYNAMIC ALLOCATION WITH YARN
─────────────────────────────────────────────────────────────────────────────
  --conf spark.dynamicAllocation.enabled=true
  --conf spark.dynamicAllocation.minExecutors=2
  --conf spark.dynamicAllocation.maxExecutors=100
  --conf spark.dynamicAllocation.initialExecutors=10
  --conf spark.shuffle.service.enabled=true   # Required for dynamic alloc!

  Note: YARN needs External Shuffle Service enabled
```

## Kubernetes Deployment

```
KUBERNETES DEPLOYMENT
═══════════════════════════════════════════════════════════════════════════════

spark-submit \
  --master k8s://https://kubernetes-api-server:6443 \
  --deploy-mode cluster \
  --name my-spark-app \
  --conf spark.kubernetes.container.image=my-spark:3.5.0 \
  --conf spark.kubernetes.namespace=spark-apps \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.executor.instances=5 \
  --conf spark.kubernetes.executor.request.cores=2 \
  --conf spark.kubernetes.executor.limit.cores=4 \
  --conf spark.kubernetes.driver.request.cores=1 \
  local:///opt/spark/work-dir/my-app.jar


K8s ARCHITECTURE
─────────────────────────────────────────────────────────────────────────────

  ┌────────────────────────────────────────────────────────────────────────┐
  │                    KUBERNETES CLUSTER                                  │
  │                                                                        │
  │  ┌────────────────────────────────────────────────────────────────┐   │
  │  │                    SPARK NAMESPACE                             │   │
  │  │                                                                │   │
  │  │  ┌─────────────────┐  ┌─────────────────┐                     │   │
  │  │  │   Driver Pod    │  │  Executor Pod 1 │                     │   │
  │  │  │  ┌───────────┐  │  │  ┌───────────┐  │                     │   │
  │  │  │  │ Spark     │  │  │  │ Spark     │  │                     │   │
  │  │  │  │ Driver    │  │  │  │ Executor  │  │                     │   │
  │  │  │  └───────────┘  │  │  └───────────┘  │                     │   │
  │  │  └─────────────────┘  └─────────────────┘                     │   │
  │  │                                                                │   │
  │  │  ┌─────────────────┐  ┌─────────────────┐                     │   │
  │  │  │  Executor Pod 2 │  │  Executor Pod N │                     │   │
  │  │  └─────────────────┘  └─────────────────┘                     │   │
  │  │                                                                │   │
  │  └────────────────────────────────────────────────────────────────┘   │
  │                                                                        │
  │  Benefits: Auto-scaling, containerization, multi-tenancy              │
  └────────────────────────────────────────────────────────────────────────┘
```

## Spark Standalone Cluster

```
STANDALONE CLUSTER
═══════════════════════════════════════════════════════════════════════════════

# Start master
$SPARK_HOME/sbin/start-master.sh

# Start workers (on each worker node)
$SPARK_HOME/sbin/start-worker.sh spark://master-host:7077

# Submit job
spark-submit \
  --master spark://master-host:7077 \
  --deploy-mode client \
  --executor-memory 4g \
  --total-executor-cores 20 \
  myapp.jar


STANDALONE ARCHITECTURE
─────────────────────────────────────────────────────────────────────────────

  ┌────────────────────────────────────────────────────────────────────────┐
  │                                                                        │
  │  ┌─────────────────┐                                                  │
  │  │   MASTER        │  spark://master:7077                             │
  │  │   (Scheduler)   │  Web UI: http://master:8080                      │
  │  └────────┬────────┘                                                  │
  │           │                                                           │
  │     ┌─────┴─────┬───────────────┐                                    │
  │     ▼           ▼               ▼                                    │
  │  ┌────────┐  ┌────────┐  ┌────────┐                                  │
  │  │Worker 1│  │Worker 2│  │Worker 3│                                  │
  │  │ Execs  │  │ Execs  │  │ Execs  │                                  │
  │  └────────┘  └────────┘  └────────┘                                  │
  │                                                                        │
  └────────────────────────────────────────────────────────────────────────┘
```

## Building Spark Applications for Deployment

```scala
// build.sbt for deployment
name := "my-spark-app"
version := "1.0.0"
scalaVersion := "2.12.18"

// Mark Spark as "provided" - cluster has it
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
)

// Create fat JAR with all dependencies
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
```

```bash
# Build the JAR
sbt assembly

# Submit to cluster
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/my-spark-app-assembly-1.0.0.jar
```

## Resource Sizing Guidelines

```
EXECUTOR SIZING
═══════════════════════════════════════════════════════════════════════════════

  SMALL EXECUTORS (2-4 cores, 4-8GB)
  ─────────────────────────────────────────────────────────────────────────────
  + Better parallelism for small tasks
  + Less wasted memory if executor fails
  - More overhead (JVM, shuffle service per executor)

  LARGE EXECUTORS (8-16 cores, 16-64GB)
  ─────────────────────────────────────────────────────────────────────────────
  + Efficient for broadcast joins (one copy per executor)
  + Less shuffle overhead
  - GC can be problematic
  - Single failure loses more work

  RECOMMENDED STARTING POINT:
  ─────────────────────────────────────────────────────────────────────────────
  --executor-cores 4-5
  --executor-memory 16g-20g
  --num-executors (total_cores / executor_cores) - reserve some for OS

  Example: 100 node cluster, 16 cores/node, 64GB RAM/node
  --executor-cores 4
  --executor-memory 15g  (leave some for OS/YARN)
  --num-executors 350    (100 * 14 / 4, leave some buffer)
```

## Monitoring Deployed Applications

```
VIEWING LOGS
═══════════════════════════════════════════════════════════════════════════════

# YARN - Get application logs
yarn logs -applicationId application_1234567890_0001

# YARN - Follow logs live
yarn logs -applicationId application_1234567890_0001 -follow

# Kubernetes
kubectl logs spark-driver-pod-name -n spark-apps

# Spark History Server (see kata-22 for details)
# Configure in spark-defaults.conf:
spark.eventLog.enabled=true
spark.eventLog.dir=hdfs:///spark-history
# Access at: http://history-server:18080


SPARK UI ACCESS
─────────────────────────────────────────────────────────────────────────────

  YARN:
  - Running apps: http://resourcemanager:8088 → click on app → Tracking URL
  - History: http://history-server:18080

  Kubernetes:
  - kubectl port-forward spark-driver-pod 4040:4040
  - Then: http://localhost:4040

  Standalone:
  - Master: http://master:8080
  - App: http://driver-host:4040
```

## Instructions

1. **Read** this README - understand deployment options
2. **Review** `src/main/scala/Exercise.scala` - deployment checklist
3. **Run**: `cd kata-21-cluster-deployment && sbt run`
4. **Practice** spark-submit commands (even locally with `--master local[*]`)

## Key Takeaways

1. **spark-submit** - Standard way to deploy applications
2. **Client mode** - Driver on your machine (interactive/debugging)
3. **Cluster mode** - Driver in cluster (production jobs)
4. **YARN** - Most common for Hadoop environments
5. **Kubernetes** - Modern, containerized deployments
6. **Standalone** - Simple cluster for testing
7. **Resource sizing** - 4-5 cores, 15-20GB per executor is a good start
8. **"provided" scope** - Mark Spark dependencies as provided for cluster

## Time
~25 minutes

## Next
Continue to [kata-22-history-server-debugging](../kata-22-history-server-debugging/)
