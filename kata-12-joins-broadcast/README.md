# Kata 12: Joins and Broadcast

## Goal
Master different join strategies in Spark and understand when to use each one.
Learn about broadcast joins, sort-merge joins, shuffle hash joins, and the cogroup operation.
Understand how to optimize joins for maximum performance.

## Why Join Strategy Matters

Joins are often the most expensive operations in distributed data processing. Choosing the
right join strategy can mean the difference between a query that runs in seconds vs hours.

```
═══════════════════════════════════════════════════════════════════════════════════════
                         JOIN PERFORMANCE IMPACT
═══════════════════════════════════════════════════════════════════════════════════════

  SCENARIO: Join 100M row table with 10K row table

  WRONG STRATEGY (Shuffle Hash Join):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  • Shuffle 100M rows across network                                             │
  │  • Shuffle 10K rows across network                                              │
  │  • Build hash maps on each partition                                            │
  │  • Time: 15 minutes                                                             │
  │  • Network: 50 GB transferred                                                   │
  └─────────────────────────────────────────────────────────────────────────────────┘

  RIGHT STRATEGY (Broadcast Hash Join):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  • Broadcast 10K rows to all executors (small!)                                 │
  │  • NO shuffle of 100M rows                                                      │
  │  • Join locally on each partition                                               │
  │  • Time: 30 seconds                                                             │
  │  • Network: 100 MB transferred                                                  │
  └─────────────────────────────────────────────────────────────────────────────────┘

  30x faster just by choosing the right strategy!

═══════════════════════════════════════════════════════════════════════════════════════
```

## Join Types (Logical)

Before discussing strategies, let's review the types of joins.

```
═══════════════════════════════════════════════════════════════════════════════════════
                              JOIN TYPES
═══════════════════════════════════════════════════════════════════════════════════════

  LEFT TABLE (employees)         RIGHT TABLE (departments)
  ┌────┬─────────┬─────────┐    ┌─────────┬─────────────┐
  │ id │  name   │ dept_id │    │ dept_id │  dept_name  │
  ├────┼─────────┼─────────┤    ├─────────┼─────────────┤
  │ 1  │ Alice   │   101   │    │   101   │ Engineering │
  │ 2  │ Bob     │   102   │    │   102   │ Marketing   │
  │ 3  │ Carol   │   103   │    │   104   │ Finance     │
  │ 4  │ Dave    │  null   │    └─────────┴─────────────┘
  └────┴─────────┴─────────┘

  INNER JOIN: Only matching rows
  ┌────┬─────────┬─────────┬─────────────┐
  │ 1  │ Alice   │   101   │ Engineering │
  │ 2  │ Bob     │   102   │ Marketing   │
  └────┴─────────┴─────────┴─────────────┘

  LEFT OUTER JOIN: All from left + matching from right
  ┌────┬─────────┬─────────┬─────────────┐
  │ 1  │ Alice   │   101   │ Engineering │
  │ 2  │ Bob     │   102   │ Marketing   │
  │ 3  │ Carol   │   103   │    null     │  ← No match, right is null
  │ 4  │ Dave    │  null   │    null     │  ← No match, right is null
  └────┴─────────┴─────────┴─────────────┘

  RIGHT OUTER JOIN: All from right + matching from left
  ┌────┬─────────┬─────────┬─────────────┐
  │ 1  │ Alice   │   101   │ Engineering │
  │ 2  │ Bob     │   102   │ Marketing   │
  │null│  null   │   104   │ Finance     │  ← No match, left is null
  └────┴─────────┴─────────┴─────────────┘

  FULL OUTER JOIN: All from both
  ┌────┬─────────┬─────────┬─────────────┐
  │ 1  │ Alice   │   101   │ Engineering │
  │ 2  │ Bob     │   102   │ Marketing   │
  │ 3  │ Carol   │   103   │    null     │
  │ 4  │ Dave    │  null   │    null     │
  │null│  null   │   104   │ Finance     │
  └────┴─────────┴─────────┴─────────────┘

  LEFT SEMI JOIN: Left rows that have match (no right columns)
  ┌────┬─────────┬─────────┐
  │ 1  │ Alice   │   101   │
  │ 2  │ Bob     │   102   │
  └────┴─────────┴─────────┘

  LEFT ANTI JOIN: Left rows that have NO match
  ┌────┬─────────┬─────────┐
  │ 3  │ Carol   │   103   │
  │ 4  │ Dave    │  null   │
  └────┴─────────┴─────────┘

  CROSS JOIN: Cartesian product (every combination)
  ┌────┬─────────┬─────────┬─────────────┐
  │ 1  │ Alice   │   101   │ Engineering │
  │ 1  │ Alice   │   102   │ Marketing   │
  │ 1  │ Alice   │   104   │ Finance     │
  │ 2  │ Bob     │   101   │ Engineering │
  │...│  ...    │   ...   │    ...      │  (12 rows total)
  └────┴─────────┴─────────┴─────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Join Strategies (Physical)

Spark has several physical strategies to execute joins.

### 1. Broadcast Hash Join (BHJ)

The fastest join when one side is small enough to fit in memory.

```
═══════════════════════════════════════════════════════════════════════════════════════
                         BROADCAST HASH JOIN
═══════════════════════════════════════════════════════════════════════════════════════

  SCENARIO: Large table (100M rows) JOIN Small table (10K rows)

  STEP 1: Collect small table to driver
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Small Table (on executors)              Driver                                 │
  │  ┌──────────────────────┐               ┌──────────────────────┐              │
  │  │ Partition 1: 3K rows │──────────────►│                      │              │
  │  │ Partition 2: 4K rows │──────────────►│  Collect all 10K     │              │
  │  │ Partition 3: 3K rows │──────────────►│  rows                │              │
  │  └──────────────────────┘               └──────────────────────┘              │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  STEP 2: Broadcast to all executors
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │                  Driver                                                         │
  │                    │                                                            │
  │    ┌───────────────┼───────────────┐                                           │
  │    │               │               │                                           │
  │    ▼               ▼               ▼                                           │
  │  ╔════════════╗  ╔════════════╗  ╔════════════╗                               │
  │  ║ Executor 1 ║  ║ Executor 2 ║  ║ Executor 3 ║                               │
  │  ║ Small tbl  ║  ║ Small tbl  ║  ║ Small tbl  ║  ← Each gets full copy        │
  │  ║ (10K rows) ║  ║ (10K rows) ║  ║ (10K rows) ║                               │
  │  ╚════════════╝  ╚════════════╝  ╚════════════╝                               │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  STEP 3: Build hash table and probe locally
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  On each executor:                                                              │
  │                                                                                 │
  │  1. Build hash map from broadcast data:                                         │
  │     ┌────────────────────────────────────┐                                     │
  │     │ Hash Map (in memory)               │                                     │
  │     │ key=101 → (Engineering)            │                                     │
  │     │ key=102 → (Marketing)              │                                     │
  │     │ key=104 → (Finance)                │                                     │
  │     └────────────────────────────────────┘                                     │
  │                                                                                 │
  │  2. Stream through large table partition:                                       │
  │     ┌────────────────────────────────────┐                                     │
  │     │ Large Table Partition (33M rows)   │                                     │
  │     │ For each row:                      │                                     │
  │     │   lookup = hashMap.get(row.key)    │  ← O(1) lookup                      │
  │     │   if (lookup != null)              │                                     │
  │     │     emit(row + lookup)             │                                     │
  │     └────────────────────────────────────┘                                     │
  │                                                                                 │
  │  NO SHUFFLE REQUIRED!                                                           │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  WHEN TO USE:
  • One table fits in memory (< spark.sql.autoBroadcastJoinThreshold, default 10MB)
  • Equi-joins (=)
  • Inner, left outer, left semi, left anti joins

  ADVANTAGES:                           DISADVANTAGES:
  • No shuffle of large table           • Limited by broadcast size
  • Very fast                           • Memory overhead on each executor
  • Low network I/O                     • Driver can become bottleneck

═══════════════════════════════════════════════════════════════════════════════════════
```

### 2. Sort-Merge Join (SMJ)

The workhorse for joining two large tables.

```
═══════════════════════════════════════════════════════════════════════════════════════
                           SORT-MERGE JOIN
═══════════════════════════════════════════════════════════════════════════════════════

  SCENARIO: Large table A (100M rows) JOIN Large table B (50M rows)

  STEP 1: Shuffle both tables by join key
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Table A                              Table B                                   │
  │  ┌─────────────────┐                 ┌─────────────────┐                       │
  │  │ Partition 1     │                 │ Partition 1     │                       │
  │  │ keys: 1,5,3,9   │                 │ keys: 2,8,4,1   │                       │
  │  └────────┬────────┘                 └────────┬────────┘                       │
  │           │                                   │                                 │
  │           │     SHUFFLE BY KEY (hash(key) % numPartitions)                      │
  │           │                                   │                                 │
  │           ▼                                   ▼                                 │
  │  ┌─────────────────────────────────────────────────────────────────────┐       │
  │  │                         AFTER SHUFFLE                                │       │
  │  │  Partition 0: A keys {0,3,6...}, B keys {0,3,6...}                  │       │
  │  │  Partition 1: A keys {1,4,7...}, B keys {1,4,7...}                  │       │
  │  │  Partition 2: A keys {2,5,8...}, B keys {2,5,8...}                  │       │
  │  │                                                                     │       │
  │  │  Same keys now on same partition!                                   │       │
  │  └─────────────────────────────────────────────────────────────────────┘       │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  STEP 2: Sort within each partition
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Partition 0:                                                                   │
  │  ┌─────────────────────┐    ┌─────────────────────┐                            │
  │  │ Table A (sorted)    │    │ Table B (sorted)    │                            │
  │  │ 0, 0, 3, 3, 6, 9... │    │ 0, 3, 3, 6, 6, 9... │                            │
  │  └─────────────────────┘    └─────────────────────┘                            │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  STEP 3: Merge (like merge sort)
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Sorted A: [0, 0, 3, 3, 6, 9, ...]                                              │
  │  Sorted B: [0, 3, 3, 6, 6, 9, ...]                                              │
  │            ↑ ↑                                                                  │
  │            │ │                                                                  │
  │            │ └── Pointer B                                                      │
  │            └──── Pointer A                                                      │
  │                                                                                 │
  │  Algorithm:                                                                     │
  │  while (A.hasNext && B.hasNext):                                                │
  │    if (A.key < B.key): advance A                                                │
  │    if (A.key > B.key): advance B                                                │
  │    if (A.key == B.key): emit match, handle duplicates                           │
  │                                                                                 │
  │  Linear scan through both sorted lists - O(n + m)                               │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  WHEN TO USE:
  • Both tables are large
  • Equi-joins
  • Tables already sorted/partitioned by join key (can skip sort!)
  • All join types supported

  ADVANTAGES:                           DISADVANTAGES:
  • Handles any size tables            • Requires shuffle of both tables
  • Efficient memory usage             • Requires sorting
  • Works with all join types          • Slower than broadcast for small tables

═══════════════════════════════════════════════════════════════════════════════════════
```

### 3. Shuffle Hash Join (SHJ)

A middle ground between broadcast and sort-merge.

```
═══════════════════════════════════════════════════════════════════════════════════════
                          SHUFFLE HASH JOIN
═══════════════════════════════════════════════════════════════════════════════════════

  SCENARIO: Medium table A (10M rows) JOIN Medium table B (5M rows)

  STEP 1: Shuffle both tables by join key (same as SMJ)
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  After shuffle, same keys are co-located:                                       │
  │                                                                                 │
  │  Partition 0: A rows with key%3=0, B rows with key%3=0                          │
  │  Partition 1: A rows with key%3=1, B rows with key%3=1                          │
  │  Partition 2: A rows with key%3=2, B rows with key%3=2                          │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  STEP 2: Build hash table from smaller side, probe with larger
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  On each partition:                                                             │
  │                                                                                 │
  │  1. Build hash map from smaller table's partition:                              │
  │     ┌────────────────────────────────────┐                                     │
  │     │ Hash Map (B partition)             │                                     │
  │     │ key=0 → [row1, row2]               │                                     │
  │     │ key=3 → [row3]                     │                                     │
  │     │ key=6 → [row4, row5, row6]         │                                     │
  │     └────────────────────────────────────┘                                     │
  │                                                                                 │
  │  2. Stream through larger table's partition:                                    │
  │     for each row in A partition:                                                │
  │       matches = hashMap.get(row.key)                                            │
  │       for match in matches:                                                     │
  │         emit(row + match)                                                       │
  │                                                                                 │
  │  NO SORTING REQUIRED!                                                           │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  WHEN TO USE:
  • One side fits in memory per partition (not total)
  • Equi-joins
  • spark.sql.join.preferSortMergeJoin = false

  ADVANTAGES:                           DISADVANTAGES:
  • No sorting required                 • Requires shuffle
  • Fast for medium-sized joins         • Hash table must fit in memory
  • Good when data is skewed            • Limited join type support

═══════════════════════════════════════════════════════════════════════════════════════
```

### 4. Broadcast Nested Loop Join (BNLJ)

For non-equi joins or when other strategies don't apply.

```
═══════════════════════════════════════════════════════════════════════════════════════
                      BROADCAST NESTED LOOP JOIN
═══════════════════════════════════════════════════════════════════════════════════════

  SCENARIO: Join with complex condition (not just =)
            A.date BETWEEN B.start_date AND B.end_date

  STEP 1: Broadcast smaller table
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Small table B broadcast to all executors (same as BHJ)                         │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  STEP 2: Nested loop comparison
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  On each executor:                                                              │
  │                                                                                 │
  │  for each row_a in A partition:         ← Outer loop                            │
  │    for each row_b in B (broadcast):     ← Inner loop                            │
  │      if (row_a.date >= row_b.start_date                                         │
  │          && row_a.date <= row_b.end_date):                                      │
  │        emit(row_a + row_b)                                                      │
  │                                                                                 │
  │  Complexity: O(n × m) - can be very slow!                                       │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  WHEN TO USE:
  • Non-equi joins (>, <, >=, <=, BETWEEN, etc.)
  • Cross joins
  • Theta joins
  • When one side is very small

  ADVANTAGES:                           DISADVANTAGES:
  • Handles any join condition          • O(n × m) complexity
  • Required for non-equi joins         • Very slow for large tables
  • Simple implementation               • High CPU usage

═══════════════════════════════════════════════════════════════════════════════════════
```

## Join Strategy Comparison

```
═══════════════════════════════════════════════════════════════════════════════════════
                      JOIN STRATEGY COMPARISON
═══════════════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────┬───────────────┬─────────────┬─────────────┬──────────────┐
  │ Strategy            │ Shuffle       │ Sort        │ Best For    │ Complexity   │
  ├─────────────────────┼───────────────┼─────────────┼─────────────┼──────────────┤
  │ Broadcast Hash      │ No            │ No          │ Small+Large │ O(n)         │
  │ (BHJ)               │               │             │             │              │
  ├─────────────────────┼───────────────┼─────────────┼─────────────┼──────────────┤
  │ Sort-Merge          │ Yes (both)    │ Yes (both)  │ Large+Large │ O(n log n)   │
  │ (SMJ)               │               │             │             │              │
  ├─────────────────────┼───────────────┼─────────────┼─────────────┼──────────────┤
  │ Shuffle Hash        │ Yes (both)    │ No          │ Medium+Med  │ O(n)         │
  │ (SHJ)               │               │             │             │              │
  ├─────────────────────┼───────────────┼─────────────┼─────────────┼──────────────┤
  │ Broadcast Nested    │ No            │ No          │ Non-equi    │ O(n × m)     │
  │ Loop (BNLJ)         │               │             │             │              │
  └─────────────────────┴───────────────┴─────────────┴─────────────┴──────────────┘

  DECISION TREE:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Is it an equi-join (=)?                                                        │
  │  ├── NO  → Broadcast Nested Loop Join (or Cartesian)                            │
  │  └── YES                                                                        │
  │       │                                                                         │
  │       Is one side small (<10MB default)?                                        │
  │       ├── YES → Broadcast Hash Join                                             │
  │       └── NO                                                                    │
  │            │                                                                    │
  │            Can one side fit in memory per partition?                            │
  │            ├── YES → Shuffle Hash Join (if preferred)                           │
  │            └── NO  → Sort-Merge Join                                            │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Using Join Hints

You can tell Spark which strategy to use with hints.

```scala
import org.apache.spark.sql.functions._

// =============================================
// BROADCAST HINT
// =============================================

// Method 1: broadcast() function
largeDF.join(broadcast(smallDF), "key")

// Method 2: DataFrame hint
largeDF.join(smallDF.hint("broadcast"), "key")

// Method 3: SQL hint
spark.sql("""
  SELECT /*+ BROADCAST(small) */ *
  FROM large
  JOIN small ON large.key = small.key
""")

// =============================================
// SORT-MERGE HINT
// =============================================

// Force sort-merge join
df1.join(df2.hint("merge"), "key")

// SQL hint
spark.sql("""
  SELECT /*+ MERGE(t1, t2) */ *
  FROM t1 JOIN t2 ON t1.key = t2.key
""")

// =============================================
// SHUFFLE HASH HINT
// =============================================

// Force shuffle hash join
df1.join(df2.hint("shuffle_hash"), "key")

// =============================================
// SHUFFLE REPLICATE NL HINT (for cross joins)
// =============================================

// Explicit nested loop
df1.join(df2.hint("shuffle_replicate_nl"))
```

## CoGroup: Low-Level Join Alternative

CoGroup is an RDD operation that groups data by key without requiring a full join.

```
═══════════════════════════════════════════════════════════════════════════════════════
                              CoGroup OPERATION
═══════════════════════════════════════════════════════════════════════════════════════

  CoGroup vs Join:

  JOIN produces: (key, leftRow, rightRow) for each matching pair
  COGROUP produces: (key, Iterator[leftRows], Iterator[rightRows])

  EXAMPLE:

  Left RDD:  (1, "a"), (1, "b"), (2, "c")
  Right RDD: (1, "x"), (1, "y"), (3, "z")

  JOIN result (inner):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  (1, "a", "x")                                                                  │
  │  (1, "a", "y")                                                                  │
  │  (1, "b", "x")                                                                  │
  │  (1, "b", "y")                                                                  │
  │                                                                                 │
  │  4 rows for key 1 (2 left × 2 right = 4 combinations)                           │
  └─────────────────────────────────────────────────────────────────────────────────┘

  COGROUP result:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  (1, ["a", "b"], ["x", "y"])    ← Key 1: both sides have data                   │
  │  (2, ["c"], [])                 ← Key 2: only left has data                     │
  │  (3, [], ["z"])                 ← Key 3: only right has data                    │
  │                                                                                 │
  │  3 records total (one per unique key)                                           │
  └─────────────────────────────────────────────────────────────────────────────────┘

  COGROUP USE CASES:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  1. Custom join logic (not just Cartesian product of matches)                   │
  │     - Find max from each side                                                   │
  │     - Complex aggregation across matched rows                                   │
  │                                                                                 │
  │  2. Implementing outer joins with custom behavior                               │
  │     - Different null handling                                                   │
  │                                                                                 │
  │  3. Avoiding data explosion                                                     │
  │     - Key with 1000 left × 1000 right = 1M rows in join                        │
  │     - Same key in cogroup = 1 row with 2 iterators                              │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

### CoGroup Example

```scala
// Using RDD cogroup
val leftRDD = sc.parallelize(Seq((1, "a"), (1, "b"), (2, "c")))
val rightRDD = sc.parallelize(Seq((1, "x"), (1, "y"), (3, "z")))

val cogrouped = leftRDD.cogroup(rightRDD)

cogrouped.collect().foreach { case (key, (leftIter, rightIter)) =>
  println(s"Key: $key")
  println(s"  Left values: ${leftIter.toList}")
  println(s"  Right values: ${rightIter.toList}")
}

// Output:
// Key: 1
//   Left values: List(a, b)
//   Right values: List(x, y)
// Key: 2
//   Left values: List(c)
//   Right values: List()
// Key: 3
//   Left values: List()
//   Right values: List(z)

// Custom join logic with cogroup
val customJoin = cogrouped.flatMap { case (key, (leftIter, rightIter)) =>
  val leftList = leftIter.toList
  val rightList = rightIter.toList

  if (leftList.nonEmpty && rightList.nonEmpty) {
    // Custom: only emit first match from each side
    Some((key, leftList.head, rightList.head))
  } else {
    None
  }
}
```

## Broadcast Variables (Beyond Joins)

Broadcast variables are useful beyond joins for sharing read-only data.

```scala
// Create broadcast variable
val countryCodeMap = Map(
  "US" -> "United States",
  "UK" -> "United Kingdom",
  "CA" -> "Canada"
)
val broadcastMap = spark.sparkContext.broadcast(countryCodeMap)

// Use in transformation
val enrichedDF = df.map { row =>
  val code = row.getAs[String]("country_code")
  val name = broadcastMap.value.getOrElse(code, "Unknown")
  (row.getAs[Int]("id"), name)
}

// Use in UDF
val lookupUDF = udf((code: String) =>
  broadcastMap.value.getOrElse(code, "Unknown")
)
df.withColumn("country_name", lookupUDF($"country_code"))

// Clean up when done
broadcastMap.unpersist()
broadcastMap.destroy()
```

## Configuration Options

```scala
// =============================================
// BROADCAST SETTINGS
// =============================================

// Auto-broadcast threshold (default 10MB)
// Tables smaller than this are automatically broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  // 10MB

// Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

// Broadcast timeout (seconds)
spark.conf.set("spark.sql.broadcastTimeout", "300")

// =============================================
// JOIN STRATEGY PREFERENCES
// =============================================

// Prefer sort-merge over shuffle hash (default: true)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

// Shuffle partitions (affects join performance)
spark.conf.set("spark.sql.shuffle.partitions", "200")

// =============================================
// SKEW JOIN OPTIMIZATION (Spark 3.0+)
// =============================================

// Enable adaptive query execution (required for skew handling)
spark.conf.set("spark.sql.adaptive.enabled", "true")

// Enable skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

// Skew detection threshold
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

## Common Pitfalls and Solutions

### Pitfall 1: Unintended Cartesian Product

```scala
// DANGEROUS: Missing join condition creates Cartesian product!
df1.join(df2)  // Without condition = CROSS JOIN

// SAFER: Always specify join condition
df1.join(df2, df1("key") === df2("key"))
df1.join(df2, "key")  // Column name shorthand

// Enable protection
spark.conf.set("spark.sql.crossJoin.enabled", "false")  // Throws error on cross join
```

### Pitfall 2: Data Skew

```scala
// PROBLEM: One key has millions of rows, others have few
// Result: One task takes 10x longer than others

// SOLUTION 1: Salting
val saltedLeft = leftDF.withColumn("salt", (rand() * 10).cast("int"))
val saltedRight = rightDF.withColumn("salt", explode(array((0 to 9).map(lit): _*)))
saltedLeft.join(saltedRight, Seq("key", "salt"))
          .drop("salt")

// SOLUTION 2: Broadcast the skewed keys separately
val skewedKeys = Set("popular_key_1", "popular_key_2")
val bcSkewedKeys = spark.sparkContext.broadcast(skewedKeys)

val normalJoin = leftDF.filter(!$"key".isin(skewedKeys.toSeq: _*))
                       .join(rightDF.filter(!$"key".isin(skewedKeys.toSeq: _*)), "key")

val skewedJoin = leftDF.filter($"key".isin(skewedKeys.toSeq: _*))
                       .join(broadcast(rightDF.filter($"key".isin(skewedKeys.toSeq: _*))), "key")

normalJoin.union(skewedJoin)

// SOLUTION 3: Use AQE skew join optimization (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Pitfall 3: Wrong Broadcast Size Estimate

```scala
// PROBLEM: Spark estimates table size incorrectly
// Result: Broadcasts huge table or doesn't broadcast small one

// SOLUTION: Use explicit hint
largeDF.join(broadcast(smallDF), "key")  // Force broadcast

// Or check/set statistics
smallDF.cache()
spark.sql("ANALYZE TABLE small COMPUTE STATISTICS")
```

### Pitfall 4: Null Keys in Joins

```scala
// PROBLEM: Null keys never match in equi-joins!
// This is standard SQL behavior

val left = Seq((1, "a"), (null.asInstanceOf[Integer], "b")).toDF("key", "val")
val right = Seq((1, "x"), (null.asInstanceOf[Integer], "y")).toDF("key", "val")

left.join(right, "key").show()
// Only shows key=1 match, nulls don't join!

// SOLUTION: Handle nulls explicitly if needed
left.join(right, left("key") === right("key") ||
                  (left("key").isNull && right("key").isNull))
```

### Pitfall 5: Duplicate Column Names After Join

```scala
// PROBLEM: Both tables have same column name
val df1 = Seq((1, "a")).toDF("id", "name")
val df2 = Seq((1, "x")).toDF("id", "name")

val joined = df1.join(df2, df1("id") === df2("id"))
// joined.select("name")  // Error: ambiguous!

// SOLUTION 1: Use Seq() syntax (deduplicates join columns)
df1.join(df2, Seq("id"))  // Only one "id" column

// SOLUTION 2: Alias the DataFrames
val a = df1.as("a")
val b = df2.as("b")
a.join(b, $"a.id" === $"b.id")
 .select($"a.id", $"a.name".as("name_a"), $"b.name".as("name_b"))

// SOLUTION 3: Drop or rename before join
df1.join(df2.withColumnRenamed("name", "name2"), "id")
```

## Best Practices

### 1. Check Join Strategy in Explain

```scala
// Always verify the join strategy
df1.join(df2, "key").explain()

// Look for:
// - BroadcastHashJoin (best for small tables)
// - SortMergeJoin (normal for large tables)
// - BroadcastNestedLoopJoin (warning: can be slow)
// - CartesianProduct (warning: very slow)
```

### 2. Size Your Broadcasts Correctly

```scala
// Measure actual size before broadcasting
val sizeInBytes = spark.sessionState.executePlan(smallDF.queryExecution.logical)
  .optimizedPlan.stats.sizeInBytes
println(s"Estimated size: ${sizeInBytes / (1024 * 1024)} MB")

// Set threshold appropriately
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50m")  // 50 MB
```

### 3. Pre-partition for Repeated Joins

```scala
// If joining same key repeatedly, pre-partition
val partitionedDF = largeDF.repartition($"key").persist()

// Subsequent joins avoid re-shuffling
partitionedDF.join(df1, "key")
partitionedDF.join(df2, "key")
partitionedDF.join(df3, "key")

partitionedDF.unpersist()
```

### 4. Use AQE for Dynamic Optimization

```scala
// Adaptive Query Execution handles many issues automatically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### 5. Filter Before Join

```scala
// SLOW: Join then filter
df1.join(df2, "key").filter($"status" === "active")

// FAST: Filter then join (Catalyst usually optimizes this, but be explicit)
df1.filter($"status" === "active").join(df2, "key")
```

## Instructions

1. **Read** this README thoroughly
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-12-joins-broadcast && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~45 minutes

## Next
Continue to [kata-13-memory-management](../kata-13-memory-management/)
