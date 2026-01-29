# Kata 04: Lazy Evaluation

## Goal
Prove that transformations are lazy and understand when computation actually happens.

## The Key Insight

**Nothing happens until you call an action!**

```
val rdd1 = sc.parallelize(1 to 1000000)  // Nothing happens
val rdd2 = rdd1.map(_ * 2)                // Nothing happens
val rdd3 = rdd2.filter(_ > 100)           // Nothing happens
val rdd4 = rdd3.map(_ + 1)                // Nothing happens

val result = rdd4.count()  // ← NOW everything executes!
```

## Why Lazy Evaluation?

```
BENEFITS OF LAZINESS:
═════════════════════

1. OPTIMIZATION
   Spark can analyze the entire pipeline before executing
   → Combine operations, skip unnecessary work

2. FAULT TOLERANCE
   By tracking the lineage (recipe), Spark can rebuild lost data
   → No need to store intermediate results

3. EFFICIENCY
   No intermediate RDDs are materialized
   → Saves memory and computation
```

## How to Prove Laziness

We can prove it with side effects (like printing or incrementing a counter):

```scala
var count = 0

val rdd = sc.parallelize(1 to 5).map { x =>
  count += 1  // Side effect
  x * 2
}

println(count)  // Prints 0! Nothing executed yet.

rdd.collect()   // NOW the map runs

println(count)  // Prints 5 (one for each element)
```

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-04-lazy-evaluation && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~15 minutes

## Next
After completing this kata, proceed to `kata-05-partitioning`
