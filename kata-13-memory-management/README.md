# Kata 13: Memory Management

## Goal
Understand Spark's memory model and how to tune it.

## Spark Memory Architecture

```
EXECUTOR MEMORY LAYOUT
═══════════════════════════════════════════════════════════════

  Total Executor Memory (spark.executor.memory)
  ┌─────────────────────────────────────────────────────────┐
  │                                                         │
  │  Reserved Memory (300MB)                                │
  │  ┌─────────────────────────────────────────────────┐   │
  │  │ System overhead                                  │   │
  │  └─────────────────────────────────────────────────┘   │
  │                                                         │
  │  Spark Memory (spark.memory.fraction = 0.6)            │
  │  ┌─────────────────────────────────────────────────┐   │
  │  │ ┌─────────────────┬─────────────────────────┐  │   │
  │  │ │  Storage Memory │  Execution Memory       │  │   │
  │  │ │  (cached RDDs)  │  (shuffles, joins, etc) │  │   │
  │  │ │  ◄── 50% ──►    │  ◄────── 50% ──────►    │  │   │
  │  │ └─────────────────┴─────────────────────────┘  │   │
  │  │        ↑ Unified: can borrow from each other ↑ │   │
  │  └─────────────────────────────────────────────────┘   │
  │                                                         │
  │  User Memory (remaining ~40%)                          │
  │  ┌─────────────────────────────────────────────────┐   │
  │  │ Data structures, UDFs, etc.                     │   │
  │  └─────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────┘
```

## Key Configurations

| Config | Default | Description |
|--------|---------|-------------|
| `spark.executor.memory` | 1g | Total executor heap |
| `spark.memory.fraction` | 0.6 | Fraction for Spark |
| `spark.memory.storageFraction` | 0.5 | Storage vs Execution |

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-13-memory-management && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~20 minutes

## Next
Continue to [kata-14-jobs-stages-tasks](../kata-14-jobs-stages-tasks/)
