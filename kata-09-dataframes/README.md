# Kata 09: DataFrames

## Goal
Understand DataFrames - Spark's structured data abstraction with schema and optimizations.

## RDD vs DataFrame

```
═══════════════════════════════════════════════════════════════════
                           RDD
═══════════════════════════════════════════════════════════════════

  - Unstructured: just objects
  - No schema information
  - Manual optimization
  - Good for: unstructured data, fine-grained control

  val rdd: RDD[(String, Int)] = sc.parallelize(List(("Alice", 25)))
  rdd.filter(_._2 > 20)  // Spark doesn't know what _2 means


═══════════════════════════════════════════════════════════════════
                        DataFrame
═══════════════════════════════════════════════════════════════════

  - Structured: rows with named columns
  - Has schema (column names and types)
  - Automatic optimization (Catalyst)
  - Good for: structured data, SQL queries, interop

  val df: DataFrame = spark.createDataFrame(List(("Alice", 25)))
    .toDF("name", "age")
  df.filter($"age" > 20)  // Spark knows "age" is an integer!

═══════════════════════════════════════════════════════════════════
```

## DataFrame Architecture

```
                     YOUR CODE
                         │
                         ▼
               ┌─────────────────┐
               │    DataFrame    │  High-level API
               │   API / SQL     │
               └────────┬────────┘
                        │
                        ▼
               ┌─────────────────┐
               │    Catalyst     │  Query optimization
               │    Optimizer    │
               └────────┬────────┘
                        │
                        ▼
               ┌─────────────────┐
               │    Tungsten     │  Memory/CPU optimization
               │     Engine      │
               └────────┬────────┘
                        │
                        ▼
               ┌─────────────────┐
               │      RDD        │  Execution layer
               └─────────────────┘
```

## Key Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| `select` | Choose columns | `df.select("name", "age")` |
| `filter/where` | Filter rows | `df.filter($"age" > 20)` |
| `groupBy` | Group rows | `df.groupBy("city").count()` |
| `agg` | Aggregate | `df.agg(sum("sales"))` |
| `join` | Combine DataFrames | `df1.join(df2, "key")` |
| `orderBy` | Sort | `df.orderBy($"age".desc)` |

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-09-dataframes && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~25 minutes

## Next
Continue to [kata-10-catalyst-optimizer](../kata-10-catalyst-optimizer/)
