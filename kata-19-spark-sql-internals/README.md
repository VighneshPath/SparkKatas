# Kata 19: Spark SQL Internals

## Goal
Deep dive into how Spark SQL processes queries from parsing to execution.

## Query Processing Pipeline

```
YOUR SQL QUERY
     │
     │  "SELECT name, SUM(amount) FROM orders WHERE status='completed' GROUP BY name"
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 1: PARSING                                                           │
│  ─────────────────────────────────────────────────────────────────────────  │
│  SQL string → Abstract Syntax Tree (AST) → Unresolved Logical Plan         │
│                                                                             │
│  Parser checks syntax but doesn't validate table/column names yet          │
│                                                                             │
│  Unresolved Plan:                                                          │
│  'Aggregate [name], [name, sum(amount)]                                    │
│   +- 'Filter (status = 'completed')                                        │
│      +- 'UnresolvedRelation [orders]                                       │
└─────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 2: ANALYSIS (Catalyst Analyzer)                                      │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Resolves references using Catalog:                                        │
│  - Table "orders" → actual schema                                          │
│  - Column "name" → orders.name (String)                                    │
│  - Column "amount" → orders.amount (Double)                                │
│                                                                             │
│  Resolved Plan:                                                            │
│  Aggregate [name#5], [name#5, sum(amount#7) AS sum(amount)#12]            │
│   +- Filter (status#6 = completed)                                         │
│      +- Relation[name#5,status#6,amount#7] parquet                        │
└─────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 3: LOGICAL OPTIMIZATION (Catalyst Optimizer)                         │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Applies optimization rules:                                               │
│  - Predicate pushdown (filter early)                                       │
│  - Column pruning (read only needed columns)                               │
│  - Constant folding (evaluate constants at compile time)                   │
│  - Boolean simplification                                                  │
│  - Join reordering                                                         │
│                                                                             │
│  Optimized Plan:                                                           │
│  Aggregate [name#5], [name#5, sum(amount#7)]                              │
│   +- Project [name#5, amount#7]           ← Only needed columns           │
│      +- Filter (status#6 = completed)     ← Filter pushed down            │
│         +- Relation[name#5,status#6,amount#7] parquet                     │
└─────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 4: PHYSICAL PLANNING                                                 │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Generates multiple physical plans and picks the best one using            │
│  cost-based optimization (CBO)                                             │
│                                                                             │
│  Physical Plan:                                                            │
│  *(2) HashAggregate(keys=[name#5], functions=[sum(amount#7)])             │
│   +- Exchange hashpartitioning(name#5, 200)    ← Shuffle                   │
│      +- *(1) HashAggregate(keys=[name#5], functions=[partial_sum(amount)]) │
│         +- *(1) Project [name#5, amount#7]                                 │
│            +- *(1) Filter (status#6 = completed)                           │
│               +- *(1) FileScan parquet [name#5,status#6,amount#7]         │
│                       PushedFilters: [EqualTo(status,completed)]           │
└─────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 5: CODE GENERATION (Tungsten)                                        │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Generates optimized Java bytecode at runtime                              │
│  Whole-stage code generation fuses operators                               │
│                                                                             │
│  *(1) = WholeStageCodegen Stage 1                                          │
│  *(2) = WholeStageCodegen Stage 2                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Catalyst Optimizer Rules

```
PREDICATE PUSHDOWN
═══════════════════════════════════════════════════════════════════════════════

    Before:                          After:
    ┌─────────────┐                  ┌─────────────┐
    │   Filter    │                  │    Join     │
    │ a.id > 100  │                  └──────┬──────┘
    └──────┬──────┘                         │
           │                         ┌──────┴──────┐
    ┌──────┴──────┐                  │             │
    │    Join     │           ┌──────┴─────┐ ┌─────┴──────┐
    └──────┬──────┘           │   Filter   │ │  Scan B    │
           │                  │ a.id > 100 │ └────────────┘
    ┌──────┴──────┐           └──────┬─────┘
    │             │                  │
  Scan A       Scan B          ┌─────┴─────┐
                               │  Scan A   │
                               └───────────┘

    Filter pushed below join = less data to join!


COLUMN PRUNING
═══════════════════════════════════════════════════════════════════════════════

    Query: SELECT name FROM users WHERE age > 21

    Before:                          After:
    ┌─────────────┐                  ┌─────────────┐
    │  Project    │                  │  Project    │
    │   [name]    │                  │   [name]    │
    └──────┬──────┘                  └──────┬──────┘
           │                                │
    ┌──────┴──────┐                  ┌──────┴──────┐
    │   Filter    │                  │   Filter    │
    │  age > 21   │                  │  age > 21   │
    └──────┬──────┘                  └──────┬──────┘
           │                                │
    ┌──────┴──────┐                  ┌──────┴──────┐
    │    Scan     │                  │    Scan     │
    │ ALL columns │                  │ [name, age] │  ← Only needed!
    └─────────────┘                  └─────────────┘


CONSTANT FOLDING
═══════════════════════════════════════════════════════════════════════════════

    Before: SELECT * FROM t WHERE 1 + 1 = 2
    After:  SELECT * FROM t WHERE true

    Before: SELECT * FROM t WHERE x > 100 + 200
    After:  SELECT * FROM t WHERE x > 300


JOIN REORDERING
═══════════════════════════════════════════════════════════════════════════════

    Tables: A (1M rows), B (100 rows), C (10K rows)

    Query: A JOIN B JOIN C

    Bad order:                       Good order:
    ┌─────────────┐                  ┌─────────────┐
    │    Join     │                  │    Join     │
    │   A ⋈ B     │ 1M × 100        │   B ⋈ C     │ 100 × 10K
    └──────┬──────┘                  └──────┬──────┘
           │                                │
    ┌──────┴──────┐                  ┌──────┴──────┐
    │    Join     │                  │    Join     │
    │  (A⋈B) ⋈ C  │                  │ (B⋈C) ⋈ A  │
    └─────────────┘                  └─────────────┘

    Small tables first = smaller intermediate results!
```

## Understanding explain() Output

```scala
df.explain(true)  // Shows all plans

// Output breakdown:
== Parsed Logical Plan ==      // Raw AST from parser
== Analyzed Logical Plan ==    // After resolving names
== Optimized Logical Plan ==   // After Catalyst optimizations
== Physical Plan ==            // Actual execution plan

// Physical Plan symbols:
*(1)        WholeStageCodegen stage 1
Exchange    Shuffle operation
HashAggregate    Hash-based aggregation
Sort        Sort operation
BroadcastExchange    Broadcast for join
```

## Adaptive Query Execution (AQE)

```
AQE = Runtime optimization based on actual data statistics
═══════════════════════════════════════════════════════════════════════════════

    Enabled by: spark.sql.adaptive.enabled = true (default in Spark 3.x)


FEATURE 1: Coalesce Shuffle Partitions
─────────────────────────────────────────────────────────────────────────────

    Initial: 200 partitions (default)

    After shuffle, AQE observes:
    - Partition 0: 10MB
    - Partition 1: 5KB    ← tiny!
    - Partition 2: 8KB    ← tiny!
    - ...
    - Partition 199: 12MB

    AQE coalesces small partitions:
    - New Partition 0: 10MB (original 0)
    - New Partition 1: 12MB (merged 1-50)
    - ...

    Result: Fewer, balanced partitions!


FEATURE 2: Convert Sort-Merge Join to Broadcast Join
─────────────────────────────────────────────────────────────────────────────

    Plan says: Sort-Merge Join (both tables "large")

    At runtime, AQE discovers:
    - Table A: 500MB (as expected)
    - Table B: 8MB (much smaller than estimated!)

    AQE switches to Broadcast Join (no shuffle needed!)


FEATURE 3: Skew Join Optimization
─────────────────────────────────────────────────────────────────────────────

    Join key distribution:
    - Key "A": 10 million rows    ← SKEW!
    - Key "B": 1000 rows
    - Key "C": 500 rows

    Without AQE: One task processes 10M rows (slow!)

    With AQE: Splits skewed partition:
    - Task 1: Key "A" rows 0-3M
    - Task 2: Key "A" rows 3M-6M
    - Task 3: Key "A" rows 6M-10M
    - Task 4: Keys "B", "C"

    Parallel processing of skewed key!
```

## Cost-Based Optimization (CBO)

```
CBO uses table statistics to make decisions
═══════════════════════════════════════════════════════════════════════════════

    Enable CBO:
    spark.sql.cbo.enabled = true

    Collect statistics:
    ANALYZE TABLE orders COMPUTE STATISTICS
    ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, amount

    Statistics collected:
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  Table: orders                                                          │
    │  Row count: 10,000,000                                                 │
    │  Size: 2.5 GB                                                          │
    │                                                                         │
    │  Column: customer_id                                                   │
    │    Distinct values: 50,000                                             │
    │    Null count: 0                                                       │
    │    Min: 1, Max: 50000                                                  │
    │                                                                         │
    │  Column: amount                                                        │
    │    Distinct values: 9,500,000                                          │
    │    Null count: 1,000                                                   │
    │    Min: 0.01, Max: 10000.00                                            │
    │    Histogram: [0-100: 40%, 100-500: 35%, 500+: 25%]                    │
    └─────────────────────────────────────────────────────────────────────────┘

    CBO uses this to estimate:
    - Join output sizes
    - Filter selectivity
    - Best join order
    - Whether to broadcast
```

## Inspecting Query Plans Programmatically

```scala
// Get the logical plan
val logicalPlan = df.queryExecution.logical
val analyzedPlan = df.queryExecution.analyzed
val optimizedPlan = df.queryExecution.optimizedPlan

// Get the physical plan
val physicalPlan = df.queryExecution.sparkPlan
val executedPlan = df.queryExecution.executedPlan

// See what optimizations were applied
df.queryExecution.optimizedPlan.numberedTreeString

// Get statistics
df.queryExecution.optimizedPlan.stats
```

## Instructions

1. **Read** this README - understand the query pipeline
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-19-spark-sql-internals && sbt run`
4. **Compare** different explain() outputs
5. **Check** `solution/Solution.scala` if needed

## Key Configurations

| Config | Default | Description |
|--------|---------|-------------|
| `spark.sql.adaptive.enabled` | true (3.x) | Enable AQE |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Coalesce small partitions |
| `spark.sql.adaptive.skewJoin.enabled` | true | Handle skewed joins |
| `spark.sql.cbo.enabled` | false | Enable cost-based optimization |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Auto-broadcast threshold |

## Time
~35 minutes

## Next
Continue to [kata-20-performance-tuning](../kata-20-performance-tuning/)
