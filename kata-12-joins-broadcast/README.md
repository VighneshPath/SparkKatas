# Kata 12: Joins and Broadcast

## Goal
Master different join strategies in Spark.

## Join Strategies

```
SHUFFLE JOIN (Default for large tables)
═══════════════════════════════════════════════════════════════

  Table A                    Table B
  ┌────────────┐            ┌────────────┐
  │ id │ value │            │ id │ name  │
  ├────┼───────┤            ├────┼───────┤
  │ 1  │  100  │            │ 1  │ Alice │
  │ 2  │  200  │            │ 2  │ Bob   │
  └────┴───────┘            └────┴───────┘
        │                          │
        └──────────┬───────────────┘
                   │
             ┌─────┴─────┐
             │  SHUFFLE  │  ← Both tables shuffled
             └─────┬─────┘
                   │
             ┌─────┴─────┐
             │   JOIN    │
             └───────────┘


BROADCAST JOIN (Small table broadcast to all)
═══════════════════════════════════════════════════════════════

  Large Table              Small Table (broadcast)
  ┌────────────┐          ╔════════════════╗
  │ id │ value │          ║ Broadcast to   ║
  ├────┼───────┤          ║ all executors  ║
  │ 1  │  100  │          ╚═══════╤════════╝
  │ 2  │  200  │                  │
  └────┴───────┘                  ▼
        │              ┌─────────────────────┐
        └──────────────│ JOIN (no shuffle!)  │
                       └─────────────────────┘
```

## Join Types

| Type | Description |
|------|-------------|
| Inner | Only matching rows |
| Left Outer | All from left, matching from right |
| Right Outer | All from right, matching from left |
| Full Outer | All from both |
| Cross | Cartesian product |

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-12-joins-broadcast && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~25 minutes

## Next
Continue to [kata-13-memory-management](../kata-13-memory-management/)
