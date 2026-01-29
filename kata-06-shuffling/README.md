# Kata 06: Shuffling

## Goal
Understand what shuffles are, when they occur, and how to minimize them.

## What is a Shuffle?

```
                        SHUFFLE = DATA MOVEMENT
═══════════════════════════════════════════════════════════════════

  A shuffle occurs when data needs to move between partitions

  BEFORE SHUFFLE (groupByKey on "color"):

  Partition 0         Partition 1         Partition 2
  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
  │ (red, 1)    │    │ (blue, 4)   │    │ (red, 7)    │
  │ (blue, 2)   │    │ (red, 5)    │    │ (green, 8)  │
  │ (green, 3)  │    │ (green, 6)  │    │ (blue, 9)   │
  └─────────────┘    └─────────────┘    └─────────────┘
         │                 │                  │
         └─────────────────┼──────────────────┘
                           │
                    ┌──────┴──────┐
                    │   SHUFFLE   │  ← Network I/O
                    │  (Expensive)│    Disk I/O
                    └──────┬──────┘    Serialization
                           │
         ┌─────────────────┼──────────────────┐
         │                 │                  │
         ▼                 ▼                  ▼
  Partition 0         Partition 1         Partition 2
  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
  │ (red, 1)    │    │ (blue, 2)   │    │ (green, 3)  │
  │ (red, 5)    │    │ (blue, 4)   │    │ (green, 6)  │
  │ (red, 7)    │    │ (blue, 9)   │    │ (green, 8)  │
  └─────────────┘    └─────────────┘    └─────────────┘
     All reds           All blues         All greens

═══════════════════════════════════════════════════════════════════
```

## Operations That Cause Shuffles

| Operation | Shuffle? | Why |
|-----------|----------|-----|
| `groupByKey` | YES | Groups all values by key |
| `reduceByKey` | YES* | Groups + reduces (* but more efficient) |
| `join` | YES | Brings matching keys together |
| `repartition` | YES | Redistributes data |
| `coalesce` | NO | Merges partitions locally |
| `map`, `filter` | NO | Per-element, no data movement |

## Shuffle Costs

```
SHUFFLE INVOLVES:
  1. Write shuffle files to disk (map side)
  2. Transfer data over network
  3. Read and merge shuffle files (reduce side)
  4. Serialization/deserialization

PERFORMANCE IMPACT:
  - Network I/O is 10-100x slower than memory
  - Disk I/O adds latency
  - GC pressure from object creation
  - Stage boundaries in execution
```

## Key Concept: reduceByKey vs groupByKey

```
groupByKey:
  [All values sent across network, then reduced]

  (a,1) (a,2) (a,3) ──shuffle──> (a, [1,2,3]) ──> (a, 6)
                    3 values sent

reduceByKey:
  [Values reduced locally first, then shuffled]

  (a,1) (a,2) ──local reduce──> (a,3) ──shuffle──> (a,6)
  (a,3)                                  1 value sent

  reduceByKey sends MUCH less data over the network!
```

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-06-shuffling && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~25 minutes

## Next
Continue to [kata-07-narrow-wide-transformations](../kata-07-narrow-wide-transformations/)
