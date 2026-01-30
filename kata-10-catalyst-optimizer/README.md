# Kata 10: Catalyst Optimizer

## Goal
Understand how Spark's Catalyst optimizer transforms and optimizes your queries.
Learn about the different optimization phases, how to read query plans using explain(),
and how to write code that takes advantage of Catalyst's optimizations.

## What is Catalyst?

Catalyst is Spark SQL's query optimization framework. It transforms your high-level queries
(whether written in DataFrame API or SQL) into efficient physical execution plans. Catalyst
is at the heart of Spark SQL's performance.

```
═══════════════════════════════════════════════════════════════════════════════════════
                           WHY CATALYST MATTERS
═══════════════════════════════════════════════════════════════════════════════════════

  WITHOUT CATALYST (RDD):                  WITH CATALYST (DataFrame/SQL):
  ┌─────────────────────────────┐         ┌─────────────────────────────┐
  │  Your code runs as-is       │         │  Your code is ANALYZED      │
  │  No automatic optimization  │         │  and OPTIMIZED before run   │
  │  You must optimize manually │         │  Spark finds best strategy  │
  └─────────────────────────────┘         └─────────────────────────────┘

  EXAMPLE - The same logical query:

  // RDD approach - runs exactly as written
  rdd.filter(row => row.getInt(0) > 10)
     .map(row => (row.getString(1), row.getInt(0)))
     .filter(_._1.startsWith("A"))

  // DataFrame approach - Catalyst optimizes this!
  df.filter($"id" > 10)
    .select($"name", $"id")
    .filter($"name".startsWith("A"))

  Catalyst will reorder filters, combine operations, and generate
  optimized bytecode - potentially 10-100x faster execution!

═══════════════════════════════════════════════════════════════════════════════════════
```

## The Catalyst Pipeline

```
═══════════════════════════════════════════════════════════════════════════════════════
                        CATALYST OPTIMIZATION PIPELINE
═══════════════════════════════════════════════════════════════════════════════════════

  YOUR QUERY (SQL or DataFrame API)
          │
          │  "SELECT name FROM people WHERE age > 20"
          │   df.select("name").filter($"age" > 20)
          │
          ▼
  ┌───────────────────────────────────────────────────────────────────────────────────┐
  │  PHASE 1: PARSING                                                                 │
  │  ──────────────────────────────────────────────────────────────────────────────── │
  │  • SQL string → Abstract Syntax Tree (AST)                                        │
  │  • DataFrame API → Unresolved Logical Plan                                        │
  │                                                                                   │
  │  Output: Unresolved Logical Plan                                                  │
  │  ┌─────────────────────────────────────────────────┐                             │
  │  │  'Filter ['age > 20]                            │  ← Unresolved: 'age         │
  │  │    +- 'Project ['name]                          │  ← Unresolved: 'name        │
  │  │       +- 'UnresolvedRelation [people]           │  ← Unresolved: people       │
  │  └─────────────────────────────────────────────────┘                             │
  └───────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
  ┌───────────────────────────────────────────────────────────────────────────────────┐
  │  PHASE 2: ANALYSIS                                                                │
  │  ──────────────────────────────────────────────────────────────────────────────── │
  │  • Resolves references using Catalog                                              │
  │  • Validates column names exist                                                   │
  │  • Determines data types                                                          │
  │  • Resolves functions                                                             │
  │                                                                                   │
  │  CATALOG LOOKUP:                                                                  │
  │  ┌─────────────────────────┐                                                     │
  │  │ people table:           │                                                     │
  │  │   - name: String        │                                                     │
  │  │   - age: Integer        │                                                     │
  │  │   - city: String        │                                                     │
  │  └─────────────────────────┘                                                     │
  │                                                                                   │
  │  Output: Analyzed Logical Plan                                                    │
  │  ┌─────────────────────────────────────────────────┐                             │
  │  │  Filter (age#12 > 20)                           │  ← Resolved: age#12:Int     │
  │  │    +- Project [name#11]                         │  ← Resolved: name#11:String │
  │  │       +- Relation[name#11,age#12,city#13]       │  ← Resolved: people table   │
  │  └─────────────────────────────────────────────────┘                             │
  └───────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
  ┌───────────────────────────────────────────────────────────────────────────────────┐
  │  PHASE 3: LOGICAL OPTIMIZATION                                                    │
  │  ──────────────────────────────────────────────────────────────────────────────── │
  │  • Apply rule-based optimizations                                                 │
  │  • Rules are applied repeatedly until plan stops changing                         │
  │                                                                                   │
  │  KEY OPTIMIZATIONS:                                                               │
  │  • Predicate Pushdown        • Constant Folding                                  │
  │  • Column Pruning            • Boolean Simplification                            │
  │  • Combine Filters           • Replace Operators                                 │
  │                                                                                   │
  │  Output: Optimized Logical Plan                                                   │
  │  ┌─────────────────────────────────────────────────┐                             │
  │  │  Project [name#11]                              │  ← Project after filter     │
  │  │    +- Filter (age#12 > 20)                      │  ← Filter pushed down       │
  │  │       +- Relation[name#11,age#12]               │  ← Only needed columns!     │
  │  └─────────────────────────────────────────────────┘                             │
  └───────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
  ┌───────────────────────────────────────────────────────────────────────────────────┐
  │  PHASE 4: PHYSICAL PLANNING                                                       │
  │  ──────────────────────────────────────────────────────────────────────────────── │
  │  • Generate multiple physical plans                                               │
  │  • Use cost model to select best plan                                             │
  │  • Choose join strategies, scan methods                                           │
  │                                                                                   │
  │  CANDIDATE PLANS (example for a join):                                            │
  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                   │
  │  │ Broadcast Hash  │  │ Sort-Merge Join │  │ Shuffle Hash    │                   │
  │  │ Join            │  │                 │  │ Join            │                   │
  │  │ Cost: 100       │  │ Cost: 500       │  │ Cost: 450       │                   │
  │  └─────────────────┘  └─────────────────┘  └─────────────────┘                   │
  │         ↑                                                                        │
  │      SELECTED (lowest cost)                                                       │
  │                                                                                   │
  │  Output: Selected Physical Plan (SparkPlan)                                       │
  └───────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
  ┌───────────────────────────────────────────────────────────────────────────────────┐
  │  PHASE 5: CODE GENERATION (Tungsten)                                              │
  │  ──────────────────────────────────────────────────────────────────────────────── │
  │  • Whole-stage code generation                                                    │
  │  • Compile query to optimized Java bytecode                                       │
  │  • Fuse operators to eliminate virtual function calls                             │
  │                                                                                   │
  │  Output: Executable RDDs with generated code                                      │
  └───────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
      EXECUTION

═══════════════════════════════════════════════════════════════════════════════════════
```

## Key Optimization Rules

### 1. Predicate Pushdown

Push filters as close to the data source as possible to reduce data early.

```
═══════════════════════════════════════════════════════════════════════════════════════
                              PREDICATE PUSHDOWN
═══════════════════════════════════════════════════════════════════════════════════════

  BEFORE OPTIMIZATION:

  SELECT name FROM (
    SELECT * FROM orders
    JOIN customers ON orders.cust_id = customers.id
  ) WHERE country = 'USA'

                    ┌──────────┐
                    │  Filter  │  country = 'USA'
                    └────┬─────┘
                         │
                    ┌────┴─────┐
                    │   Join   │  ALL rows joined first!
                    └────┬─────┘
               ┌─────────┴─────────┐
          ┌────┴────┐         ┌────┴────┐
          │ Orders  │         │Customers│
          │(1M rows)│         │(100K)   │
          └─────────┘         └─────────┘

  Processing: 1M x 100K = potential 100 billion comparisons!


  AFTER PREDICATE PUSHDOWN:

                    ┌──────────┐
                    │  Project │  name only
                    └────┬─────┘
                         │
                    ┌────┴─────┐
                    │   Join   │  Much smaller join!
                    └────┬─────┘
               ┌─────────┴─────────┐
          ┌────┴────┐         ┌────┴────┐
          │ Orders  │         │ Filter  │  country = 'USA' (pushed!)
          │(1M rows)│         └────┬────┘
          └─────────┘              │
                             ┌────┴────┐
                             │Customers│
                             │(10K USA)│  ← 90% filtered out!
                             └─────────┘

  Processing: 1M x 10K = 10 million comparisons (100x faster!)

═══════════════════════════════════════════════════════════════════════════════════════
```

### 2. Column Pruning (Projection Pushdown)

Only read columns that are actually needed.

```
═══════════════════════════════════════════════════════════════════════════════════════
                              COLUMN PRUNING
═══════════════════════════════════════════════════════════════════════════════════════

  YOUR QUERY:
  df.select("name", "age")  // Only need 2 of 20 columns

  BEFORE (read all columns):
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Parquet File                                                           │
  │  ┌────┬─────┬────┬────────┬───────┬────────┬─────────┬────┬─────┬────┐│
  │  │ id │name │age │address │ phone │ email  │ salary  │dept│mgr  │... ││
  │  ├────┼─────┼────┼────────┼───────┼────────┼─────────┼────┼─────┼────┤│
  │  │ .. │ ... │... │ ...    │ ...   │ ...    │ ...     │... │ ... │... ││
  │  └────┴─────┴────┴────────┴───────┴────────┴─────────┴────┴─────┴────┘│
  │           Read 10 GB from disk                                         │
  └─────────────────────────────────────────────────────────────────────────┘

  AFTER COLUMN PRUNING:
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Parquet File                                                           │
  │  ┌────┬─────┬────┬────────┬───────┬────────┬─────────┬────┬─────┬────┐│
  │  │ id │name │age │address │ phone │ email  │ salary  │dept│mgr  │... ││
  │  ├────┼─────┼────┼────────┼───────┼────────┼─────────┼────┼─────┼────┤│
  │  │    │ ✓✓✓ │ ✓✓ │        │       │        │         │    │     │    ││
  │  └────┴─────┴────┴────────┴───────┴────────┴─────────┴────┴─────┴────┘│
  │           Read only 1 GB from disk (90% reduction!)                    │
  └─────────────────────────────────────────────────────────────────────────┘

  This works especially well with columnar formats like Parquet!

═══════════════════════════════════════════════════════════════════════════════════════
```

### 3. Constant Folding

Evaluate constant expressions at compile time.

```scala
// BEFORE: Runtime evaluation
df.filter($"price" * 1.08 > 100 * 1.5)

// AFTER CONSTANT FOLDING: Compile-time evaluation
df.filter($"price" * 1.08 > 150.0)  // 100 * 1.5 evaluated at compile time
```

### 4. Boolean Simplification

Simplify boolean expressions.

```scala
// BEFORE
df.filter($"a" && true)       // → df.filter($"a")
df.filter($"a" || true)       // → df.filter(lit(true)) → all rows
df.filter($"a" && false)      // → df.filter(lit(false)) → no rows
df.filter(!(!$"a"))           // → df.filter($"a")
```

### 5. Combine Filters

Merge adjacent filters into one.

```scala
// BEFORE: Two filter operations
df.filter($"age" > 20).filter($"city" === "NYC")

// AFTER: Single combined filter
df.filter($"age" > 20 && $"city" === "NYC")
```

### 6. Join Reordering

Reorder joins to minimize intermediate data.

```
═══════════════════════════════════════════════════════════════════════════════════════
                              JOIN REORDERING
═══════════════════════════════════════════════════════════════════════════════════════

  TABLES:
  • orders:    10 million rows
  • items:     100 million rows
  • customers: 50,000 rows

  ORIGINAL QUERY:
  orders.join(items).join(customers)

  BEFORE OPTIMIZATION:
  ┌────────────────────────────┐
  │         Join               │
  │    ┌─────────┴────────┐    │
  │    │                  │    │
  │  Join              customers
  │ ┌──┴───┐           (50K)
  │orders  items
  │(10M)  (100M)
  │
  │ First join: 10M x 100M = huge!
  └────────────────────────────┘

  AFTER REORDERING:
  ┌────────────────────────────┐
  │         Join               │
  │    ┌─────────┴────────┐    │
  │    │                  │    │
  │  Join               items
  │ ┌──┴───┐           (100M)
  │orders  customers
  │(10M)  (50K)
  │
  │ First join: 10M x 50K = much smaller!
  │ Then join result with items (filtered)
  └────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Using explain() to Understand Plans

The `explain()` method shows you what Catalyst is doing with your query.

### Basic explain()

```scala
val df = spark.read.parquet("customers")
  .filter($"age" > 25)
  .select("name", "city")
  .groupBy("city")
  .count()

df.explain()
// Shows only the physical plan
```

### explain(true) - Extended

```scala
df.explain(true)
// Shows:
// == Parsed Logical Plan ==
// == Analyzed Logical Plan ==
// == Optimized Logical Plan ==
// == Physical Plan ==
```

### explain("mode") - Detailed Modes

```scala
// Simple physical plan
df.explain("simple")

// Extended (all plans)
df.explain("extended")

// Code generation info
df.explain("codegen")

// Cost information
df.explain("cost")

// Formatted output (Spark 3.0+)
df.explain("formatted")
```

### Reading Physical Plans

```
═══════════════════════════════════════════════════════════════════════════════════════
                         READING PHYSICAL PLANS
═══════════════════════════════════════════════════════════════════════════════════════

  == Physical Plan ==
  *(2) HashAggregate(keys=[city#15], functions=[count(1)])
  +- Exchange hashpartitioning(city#15, 200)
     +- *(1) HashAggregate(keys=[city#15], functions=[partial_count(1)])
        +- *(1) Project [city#15]
           +- *(1) Filter (age#14 > 25)
              +- *(1) ColumnarToRow
                 +- FileScan parquet [name#13,age#14,city#15]

  READING GUIDE:

  *(1), *(2)  = Whole-stage codegen pipeline numbers
                Operations in same pipeline are fused

  +- child   = Tree structure showing operator hierarchy
               Read from bottom up!

  Exchange   = Shuffle operation (data movement between executors)
               This is often a performance bottleneck!

  FileScan   = Reading from disk
               PushedFilters shows predicates pushed to source

  HashAggregate = Aggregation using hash map
                  partial_count = map-side aggregation
                  count(1) = final aggregation

═══════════════════════════════════════════════════════════════════════════════════════
```

### Key Operators in Physical Plans

| Operator | Description | Performance Notes |
|----------|-------------|-------------------|
| `FileScan` | Read from disk | Check PushedFilters |
| `Filter` | Filter rows | Should be pushed down |
| `Project` | Select columns | Should be pushed down |
| `Exchange` | Shuffle data | EXPENSIVE! Minimize these |
| `Sort` | Sort data | Requires memory |
| `HashAggregate` | Hash-based aggregation | Efficient for groupBy |
| `SortAggregate` | Sort-based aggregation | When hash doesn't fit |
| `BroadcastHashJoin` | Broadcast small table | Fast, no shuffle |
| `SortMergeJoin` | Sort both sides, merge | Handles large tables |
| `BroadcastNestedLoopJoin` | Nested loop with broadcast | For non-equi joins |

## Practical Examples

### Example 1: Seeing Predicate Pushdown

```scala
// Create a Parquet file
val customers = Seq(
  (1, "Alice", 30, "NYC"),
  (2, "Bob", 25, "LA"),
  (3, "Carol", 35, "NYC")
).toDF("id", "name", "age", "city")
customers.write.parquet("/tmp/customers")

// Read and filter
val df = spark.read.parquet("/tmp/customers")
  .filter($"age" > 25)
  .select("name")

df.explain(true)

// Output shows PushedFilters in FileScan:
// FileScan parquet [name#0,age#1]
//   PushedFilters: [IsNotNull(age), GreaterThan(age,25)]
//   ReadSchema: struct<name:string>  ← Only reading name and age!
```

### Example 2: Join Optimization

```scala
val orders = spark.range(1000000).toDF("order_id")
  .withColumn("cust_id", ($"order_id" % 1000).cast("long"))

val customers = spark.range(1000).toDF("cust_id")
  .withColumn("name", lit("Customer"))

// Without hint - Spark decides
orders.join(customers, "cust_id").explain()
// Likely shows BroadcastHashJoin (customers is small)

// Force broadcast
orders.join(broadcast(customers), "cust_id").explain()
// Definitely shows BroadcastHashJoin

// Force sort-merge (not recommended for small tables)
orders.join(customers.hint("merge"), "cust_id").explain()
// Shows SortMergeJoin
```

### Example 3: Understanding Aggregation Plans

```scala
val sales = Seq(
  ("Electronics", 100),
  ("Electronics", 200),
  ("Clothing", 50),
  ("Clothing", 75)
).toDF("category", "amount")

sales.groupBy("category").agg(sum("amount")).explain()

// == Physical Plan ==
// *(2) HashAggregate(keys=[category#0], functions=[sum(amount#1)])
// +- Exchange hashpartitioning(category#0, 200)
//    +- *(1) HashAggregate(keys=[category#0], functions=[partial_sum(amount#1)])
//       +- *(1) LocalTableScan [category#0, amount#1]
//
// TWO-PHASE AGGREGATION:
// 1. partial_sum - local aggregation on each partition (before shuffle)
// 2. sum - final aggregation after shuffle
// This reduces data transferred during shuffle!
```

### Example 4: Whole-Stage Code Generation

```scala
df.explain("codegen")

// Shows generated Java code that fuses operations
// Look for "WholeStageCodegen" in the plan
// Operations in the same WholeStageCodegen block are fused

// *(...) notation in physical plan indicates WholeStageCodegen
// *(1) Filter, *(1) Project = fused into one function
```

## Inspecting Plans Programmatically

```scala
// Get the query execution object
val queryExecution = df.queryExecution

// Access different plans
val parsedPlan = queryExecution.logical        // Unresolved logical plan
val analyzedPlan = queryExecution.analyzed     // Resolved logical plan
val optimizedPlan = queryExecution.optimizedPlan  // After logical optimization
val physicalPlan = queryExecution.sparkPlan    // Physical plan
val executedPlan = queryExecution.executedPlan // With code generation

// Print plans
println(parsedPlan.numberedTreeString)
println(optimizedPlan.numberedTreeString)

// Check plan statistics (estimated row counts, sizes)
println(analyzedPlan.stats)
```

## Cost-Based Optimization (CBO)

Spark can use statistics to make better optimization decisions.

```scala
// Enable cost-based optimization
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

// Compute statistics for a table
spark.sql("ANALYZE TABLE customers COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE customers COMPUTE STATISTICS FOR COLUMNS age, city")

// View statistics
spark.sql("DESCRIBE EXTENDED customers").show()

// Explain with cost
df.explain("cost")
// Shows estimated rows and sizes at each step
```

## Common Pitfalls and Solutions

### Pitfall 1: Breaking Optimization with UDFs

```scala
// BAD: UDF prevents predicate pushdown
val myFilter = udf((age: Int) => age > 25)
df.filter(myFilter($"age"))  // Cannot push to Parquet!

// GOOD: Use built-in expressions
df.filter($"age" > 25)  // Catalyst can optimize this!
```

### Pitfall 2: Wrong Order of Operations

```scala
// SUBOPTIMAL: Join before filter
orders.join(customers, "cust_id")
      .filter($"country" === "USA")

// BETTER: Filter before join (Catalyst often fixes this, but be explicit)
orders.join(
  customers.filter($"country" === "USA"),
  "cust_id"
)
```

### Pitfall 3: Not Using Broadcast for Small Tables

```scala
// SLOW: Shuffle join when broadcast would be better
largeDF.join(smallDF, "key")  // May use shuffle if smallDF size unknown

// FAST: Explicit broadcast
largeDF.join(broadcast(smallDF), "key")

// Configure auto-broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")  // 100 MB
```

### Pitfall 4: Ignoring Data Skew

```scala
// PROBLEM: Skewed key causes one task to be much slower
df.groupBy("popular_category").count()  // One reducer gets most data

// SOLUTION 1: Salting
df.withColumn("salt", (rand() * 10).cast("int"))
  .groupBy("category", "salt").count()
  .groupBy("category").agg(sum("count"))

// SOLUTION 2: Two-phase aggregation (often done automatically)
// Check the plan for partial aggregation
```

### Pitfall 5: Not Caching After Expensive Operations

```scala
// BAD: Recomputes expensive join/filter multiple times
val filtered = hugeDF.filter($"type" === "A")
val agg1 = filtered.groupBy("x").count()
val agg2 = filtered.groupBy("y").count()
// filtered is computed twice!

// GOOD: Cache before branching
val filtered = hugeDF.filter($"type" === "A").cache()
val agg1 = filtered.groupBy("x").count()
val agg2 = filtered.groupBy("y").count()
filtered.unpersist()
```

## Best Practices

### 1. Always Check the Plan

```scala
// Before running expensive operations, check the plan
myComplexQuery.explain(true)

// Look for:
// - Unnecessary shuffles (Exchange)
// - Missing predicate pushdown
// - Inefficient join strategies
// - Missing whole-stage codegen (*)
```

### 2. Help the Optimizer with Statistics

```scala
// Compute statistics for better CBO
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")

// For critical columns used in filters/joins
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS key_col, filter_col")
```

### 3. Use Broadcast Hints Wisely

```scala
// When you know a table is small
val result = largeDF.join(broadcast(smallDF), "key")

// Configure threshold for automatic broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50m")
```

### 4. Avoid Operations That Prevent Optimization

```scala
// AVOID: These break optimization
df.rdd.map(...)           // Converts to RDD, loses Catalyst
df.map(row => ...)        // May prevent some optimizations
complexUDF($"column")     // Black box to optimizer

// PREFER: Built-in functions
import org.apache.spark.sql.functions._
df.select(upper($"name"))  // Optimizable!
df.filter($"age" > 25)     // Pushable!
```

### 5. Partition Data Appropriately

```scala
// Write data partitioned by frequently-filtered columns
df.write
  .partitionBy("year", "month")
  .parquet("/path/to/data")

// Spark can then skip entire partitions (partition pruning)
spark.read.parquet("/path/to/data")
     .filter($"year" === 2023)  // Only reads 2023 partition!
```

## Debugging Query Performance

```scala
// 1. Check the physical plan
df.explain("formatted")

// 2. Look for warning signs:
//    - Multiple Exchange operations
//    - Missing * (whole-stage codegen)
//    - BroadcastNestedLoopJoin (usually bad)
//    - CartesianProduct (usually bad)

// 3. Check Spark UI for:
//    - Task duration distribution (skew?)
//    - Shuffle read/write sizes
//    - GC time

// 4. Try different approaches and compare plans
val approach1 = df.groupBy(...).agg(...)
val approach2 = df.repartition($"key").groupBy(...).agg(...)

approach1.explain()
approach2.explain()
```

## Summary: Catalyst Optimization Rules

| Rule | What It Does | Benefit |
|------|-------------|---------|
| Predicate Pushdown | Push filters to data source | Less data read |
| Column Pruning | Only read needed columns | Less I/O |
| Constant Folding | Evaluate constants at compile time | Faster filters |
| Boolean Simplification | Simplify boolean expressions | Fewer operations |
| Combine Filters | Merge adjacent filters | Fewer operators |
| Join Reordering | Optimize join order | Smaller intermediates |
| Limit Pushdown | Push LIMIT to children | Read less data |
| Star Schema Detection | Optimize star schema queries | Better join order |

## Instructions

1. **Read** this README thoroughly
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-10-catalyst-optimizer && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~45 minutes

## Next
Continue to [kata-11-tungsten-engine](../kata-11-tungsten-engine/)
