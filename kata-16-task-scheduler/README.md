# Kata 16: Task Scheduler

## Goal
Understand how Spark schedules tasks across executors and manages resource allocation.

## The Scheduling Pipeline

```
ACTION CALLED
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DAG SCHEDULER                               │
│  - Builds DAG of stages                                         │
│  - Submits stages in dependency order                           │
│  - Creates TaskSet for each stage                               │
└─────────────────────────────────────────────────────────────────┘
     │
     │ TaskSet (collection of tasks for one stage)
     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      TASK SCHEDULER                              │
│  - Receives TaskSets from DAG Scheduler                         │
│  - Assigns tasks to executors                                   │
│  - Handles task failures and retries                            │
│  - Manages speculative execution                                │
└─────────────────────────────────────────────────────────────────┘
     │
     │ Individual tasks
     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SCHEDULER BACKEND                             │
│  - Communicates with cluster manager (YARN/Mesos/K8s/Standalone)│
│  - Launches tasks on executors                                  │
│  - Monitors executor health                                     │
└─────────────────────────────────────────────────────────────────┘
```

## Task Assignment Strategy

```
LOCALITY-AWARE SCHEDULING
═══════════════════════════════════════════════════════════════════

Spark tries to schedule tasks where data already exists:

    Priority Order:
    ───────────────────────────────────────────────────────────────
    1. PROCESS_LOCAL  → Data in same executor's memory
                        (cached RDD partition)
                        Latency: ~0ms

    2. NODE_LOCAL     → Data on same node (HDFS block, disk cache)
                        Latency: ~1-2ms

    3. NO_PREF        → Data location doesn't matter
                        (e.g., loading from S3)

    4. RACK_LOCAL     → Data in same rack
                        Latency: ~5-10ms

    5. ANY            → Data anywhere in cluster
                        Latency: ~50-100ms+

    ───────────────────────────────────────────────────────────────

    EXAMPLE:
    ┌──────────────────────────────────────────────────────────────┐
    │  Node 1                     Node 2                          │
    │  ┌───────────────┐         ┌───────────────┐               │
    │  │  Executor A   │         │  Executor B   │               │
    │  │  ┌─────────┐  │         │  ┌─────────┐  │               │
    │  │  │Partition│  │         │  │Partition│  │               │
    │  │  │   0     │◄─┼─ Task 0 │  │   1     │◄─┼─ Task 1      │
    │  │  └─────────┘  │         │  └─────────┘  │               │
    │  └───────────────┘         └───────────────┘               │
    │                                                             │
    │  Task 0 scheduled on Executor A (PROCESS_LOCAL)            │
    │  Task 1 scheduled on Executor B (PROCESS_LOCAL)            │
    └──────────────────────────────────────────────────────────────┘
```

## Locality Wait

```
spark.locality.wait = 3s (default)

Timeline:
───────────────────────────────────────────────────────────────────
    0s                    3s                    6s
    │                     │                     │
    ▼                     ▼                     ▼
    Try PROCESS_LOCAL     Try NODE_LOCAL        Try RACK_LOCAL
    │                     │                     │
    │ wait 3s             │ wait 3s             │ wait 3s
    │                     │                     │
    └─────────────────────┴─────────────────────┴───► Try ANY

If no PROCESS_LOCAL slot available after 3s, fall back to NODE_LOCAL
If no NODE_LOCAL slot available after 3s, fall back to RACK_LOCAL
And so on...

TUNING:
- Decrease spark.locality.wait for faster scheduling (less locality)
- Increase for better data locality (more waiting)
- Set to 0 to disable locality awareness entirely
```

## Speculative Execution

```
PROBLEM: Stragglers - slow tasks that delay job completion

    Normal tasks:  ████████████ (10s)
    Straggler:     ████████████████████████████████████ (30s)
                                                      ↑
                                                Job waits here!

SOLUTION: Speculative Execution (spark.speculation = true)

    Task 1: ████████████ ✓ (10s)
    Task 2: ████████████ ✓ (10s)
    Task 3: ██████████████████████...  (straggler)
                  │
                  │ After 75% of tasks complete,
                  │ launch speculative copy
                  ▼
    Task 3': ████████████ ✓ (10s on different executor)
                  │
                  └─► First to finish wins, other killed

CONFIGURATION:
  spark.speculation = true
  spark.speculation.interval = 100ms    # How often to check
  spark.speculation.multiplier = 1.5    # Task is straggler if
                                        # > 1.5x median duration
  spark.speculation.quantile = 0.75     # Start speculating after
                                        # 75% tasks complete
```

## Fair vs FIFO Scheduling

```
FIFO SCHEDULING (Default)
═══════════════════════════════════════════════════════════════════
Jobs run in order they were submitted

    Time ─────────────────────────────────────────────────────────►

    Job 1: ████████████████████████████
    Job 2:                             ████████████
    Job 3:                                         ████

    Job 2 waits for Job 1 to complete entirely


FAIR SCHEDULING
═══════════════════════════════════════════════════════════════════
Jobs share resources, all make progress

    Time ─────────────────────────────────────────────────────────►

    Job 1: ████    ████    ████    ████
    Job 2:     ████    ████    ████
    Job 3:         ██      ██      ██

    All jobs make progress concurrently!

ENABLE FAIR SCHEDULING:
  spark.scheduler.mode = FAIR

USE POOLS FOR PRIORITY:
  spark.scheduler.pool = "production"  # Assign job to pool

  <!-- fairscheduler.xml -->
  <pool name="production">
    <schedulingMode>FAIR</schedulingMode>
    <weight>2</weight>           <!-- 2x resources -->
    <minShare>4</minShare>       <!-- Minimum 4 cores -->
  </pool>
```

## Dynamic Resource Allocation

```
STATIC ALLOCATION (Default)
═══════════════════════════════════════════════════════════════════
Fixed executors for entire application lifetime

    Request: 10 executors

    Time ─────────────────────────────────────────────────────────►

    Executors: ██████████ (10 executors, always)
    Workload:  ████████████████        ██      ████████████
                Heavy work       Idle    Light    Heavy
                                   ↑
                              Wasting resources!


DYNAMIC ALLOCATION (spark.dynamicAllocation.enabled = true)
═══════════════════════════════════════════════════════════════════
Executors scale with workload

    Time ─────────────────────────────────────────────────────────►

    Executors: ██████████     ██      ████████████
    Workload:  ████████████████        ██      ████████████
                Heavy work       Idle    Light    Heavy
                   ↑               ↑         ↑
              Scale up        Scale down  Scale up

CONFIGURATION:
  spark.dynamicAllocation.enabled = true
  spark.dynamicAllocation.minExecutors = 1
  spark.dynamicAllocation.maxExecutors = 100
  spark.dynamicAllocation.executorIdleTimeout = 60s
  spark.dynamicAllocation.schedulerBacklogTimeout = 1s
```

## Task Failure Handling

```
TASK FAILURE FLOW
═══════════════════════════════════════════════════════════════════

    Task fails
        │
        ▼
    Retry on same executor (up to spark.task.maxFailures = 4)
        │
        │ If still failing
        ▼
    Retry on different executor
        │
        │ If executor repeatedly fails
        ▼
    Blacklist executor (spark.blacklist.enabled = true)
        │
        │ If all retries exhausted
        ▼
    Stage fails → Job fails → Application can continue or fail


BLACKLISTING:
  spark.blacklist.enabled = true
  spark.blacklist.task.maxTaskAttemptsPerExecutor = 2
  spark.blacklist.task.maxTaskAttemptsPerNode = 4
  spark.blacklist.stage.maxFailedTasksPerExecutor = 2
  spark.blacklist.application.maxFailedTasksPerExecutor = 4
```

## Instructions

1. **Read** this README - understand scheduling strategies
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-16-task-scheduler && sbt run`
4. **Open** Spark UI at http://localhost:4040 to observe scheduling
5. **Check** `solution/Solution.scala` if needed

## Key Configurations Summary

| Config | Default | Description |
|--------|---------|-------------|
| `spark.locality.wait` | 3s | Wait time before lowering locality |
| `spark.speculation` | false | Enable speculative execution |
| `spark.scheduler.mode` | FIFO | FIFO or FAIR |
| `spark.task.maxFailures` | 4 | Max retries per task |
| `spark.dynamicAllocation.enabled` | false | Dynamic executor scaling |

## Time
~30 minutes

## Next
Continue to [kata-17-serialization](../kata-17-serialization/)
