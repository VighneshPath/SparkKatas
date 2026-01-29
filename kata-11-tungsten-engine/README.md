# Kata 11: Tungsten Engine

## Goal
Understand Spark's Tungsten execution engine and its optimizations.

## What is Tungsten?

```
TUNGSTEN = SPARK'S EXECUTION ENGINE
═══════════════════════════════════════════════════════════════

  Project Tungsten focuses on:
  1. Memory Management - Off-heap, binary format
  2. Cache-aware Computation - CPU-friendly data layout
  3. Code Generation - Runtime bytecode compilation

  Traditional Approach:
  ┌────────────────────────────────────────────────────────┐
  │  JVM Objects → Serialization → Memory → Deserialization│
  │  (Slow, GC pressure, memory overhead)                  │
  └────────────────────────────────────────────────────────┘

  Tungsten Approach:
  ┌────────────────────────────────────────────────────────┐
  │  UnsafeRow (Binary) → Direct Memory Access             │
  │  (Fast, minimal GC, compact storage)                   │
  └────────────────────────────────────────────────────────┘
```

## Key Optimizations

| Feature | Description |
|---------|-------------|
| Whole-Stage CodeGen | Compiles query plan to single function |
| UnsafeRow | Compact binary row format |
| Off-heap Memory | Avoids GC overhead |
| Cache-aware Algorithms | CPU L1/L2/L3 cache friendly |

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-11-tungsten-engine && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~25 minutes

## Next
Continue to [kata-12-joins-broadcast](../kata-12-joins-broadcast/)
