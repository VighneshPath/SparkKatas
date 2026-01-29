# Kata 07: Narrow vs Wide Transformations

## Goal
Understand the difference between narrow and wide transformations and their performance implications.

## Narrow vs Wide Transformations

```
═══════════════════════════════════════════════════════════════════
                    NARROW TRANSFORMATIONS
═══════════════════════════════════════════════════════════════════

  Each output partition depends on ONE input partition
  → No data movement between partitions
  → Can be pipelined in a single stage

  Parent RDD          Child RDD
  ┌─────────┐        ┌─────────┐
  │  Part 0 │───────>│  Part 0 │   map, filter, flatMap
  └─────────┘        └─────────┘
  ┌─────────┐        ┌─────────┐
  │  Part 1 │───────>│  Part 1 │   One-to-one mapping
  └─────────┘        └─────────┘
  ┌─────────┐        ┌─────────┐
  │  Part 2 │───────>│  Part 2 │   No shuffle needed!
  └─────────┘        └─────────┘


═══════════════════════════════════════════════════════════════════
                     WIDE TRANSFORMATIONS
═══════════════════════════════════════════════════════════════════

  Each output partition depends on MULTIPLE input partitions
  → Data must move between partitions (shuffle)
  → Creates a stage boundary

  Parent RDD          Child RDD
  ┌─────────┐        ┌─────────┐
  │  Part 0 │──┬──┬─>│  Part 0 │   groupByKey, reduceByKey
  └─────────┘  │  │  └─────────┘
  ┌─────────┐  │  │  ┌─────────┐
  │  Part 1 │──┼──┼─>│  Part 1 │   join, repartition
  └─────────┘  │  │  └─────────┘
  ┌─────────┐  │  │  ┌─────────┐
  │  Part 2 │──┴──┴─>│  Part 2 │   Shuffle required!
  └─────────┘        └─────────┘

═══════════════════════════════════════════════════════════════════
```

## Classification

| Narrow (No Shuffle) | Wide (Shuffle) |
|---------------------|----------------|
| `map` | `groupByKey` |
| `filter` | `reduceByKey` |
| `flatMap` | `aggregateByKey` |
| `mapPartitions` | `join` |
| `mapValues` | `cogroup` |
| `union` | `repartition` |
| `coalesce` | `distinct` |
| `sample` | `sortByKey` |

## Why Does This Matter?

```
NARROW TRANSFORMATIONS:
  ✓ Can be pipelined together
  ✓ No network I/O
  ✓ Fast execution

WIDE TRANSFORMATIONS:
  ✗ Create stage boundaries
  ✗ Require shuffle (network + disk)
  ✗ Can be slow with large data

OPTIMIZATION TIP:
  Chain narrow transformations together
  Minimize wide transformations
  Use reduceByKey instead of groupByKey
```

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-07-narrow-wide-transformations && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~20 minutes

## Next
Continue to [kata-08-caching-persistence](../kata-08-caching-persistence/)
