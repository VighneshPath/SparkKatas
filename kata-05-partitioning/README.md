# Kata 05: Partitioning

## Goal
Understand how data is distributed across partitions and why it matters for performance.

## Why Partitioning Matters

```
                    PARALLELISM = PARTITIONS
═══════════════════════════════════════════════════════════════════

  RDD with 4 partitions = 4 tasks running in parallel

  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐
  │ Partition │  │ Partition │  │ Partition │  │ Partition │
  │     0     │  │     1     │  │     2     │  │     3     │
  │  [1-25]   │  │  [26-50]  │  │  [51-75]  │  │ [76-100]  │
  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
        │              │              │              │
        ▼              ▼              ▼              ▼
     Task 0         Task 1         Task 2         Task 3
   (Core 1)       (Core 2)       (Core 3)       (Core 4)

  More partitions → More parallelism (up to available cores)
  Fewer partitions → Less parallelism, potential bottleneck

═══════════════════════════════════════════════════════════════════
```

## Key Concepts

| Concept | Description |
|---------|-------------|
| `getNumPartitions` | Returns number of partitions |
| `glom()` | Collects each partition into an array |
| `repartition(n)` | Redistributes to n partitions (shuffle) |
| `coalesce(n)` | Reduces to n partitions (no shuffle) |
| Hash Partitioner | partition = hash(key) % numPartitions |
| Range Partitioner | Divides sorted key space into ranges |

## Partition Guidelines

```
TOO FEW PARTITIONS:
  - Underutilizes cluster
  - Large partitions may cause OOM

TOO MANY PARTITIONS:
  - Scheduling overhead
  - Small files problem

RULE OF THUMB:
  - 2-4 partitions per CPU core
  - Each partition ~100-200MB
```

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-05-partitioning && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~20 minutes

## Next
Continue exploring more katas!
