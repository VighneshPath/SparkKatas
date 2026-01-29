# Kata 10: Catalyst Optimizer

## Goal
Understand how Spark's Catalyst optimizer transforms and optimizes your queries.

## What is Catalyst?

```
═══════════════════════════════════════════════════════════════════
                    CATALYST OPTIMIZER
═══════════════════════════════════════════════════════════════════

  Catalyst is Spark SQL's query optimization framework.
  It transforms your high-level queries into efficient execution plans.

  YOUR QUERY
      │
      ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  1. PARSING                                                 │
  │     SQL/DataFrame → Unresolved Logical Plan                 │
  │     "SELECT name FROM people WHERE age > 20"                │
  │         → Filter(age > 20, Project(name, Relation(people))) │
  └─────────────────────────────────────────────────────────────┘
      │
      ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  2. ANALYSIS                                                │
  │     Resolve column names, table names, types                │
  │     Check that "age" exists and is numeric                  │
  └─────────────────────────────────────────────────────────────┘
      │
      ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  3. LOGICAL OPTIMIZATION                                    │
  │     Apply optimization rules:                               │
  │     - Predicate pushdown                                    │
  │     - Column pruning                                        │
  │     - Constant folding                                      │
  └─────────────────────────────────────────────────────────────┘
      │
      ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  4. PHYSICAL PLANNING                                       │
  │     Choose execution strategy:                              │
  │     - Which join algorithm?                                 │
  │     - Broadcast or shuffle?                                 │
  └─────────────────────────────────────────────────────────────┘
      │
      ▼
  OPTIMIZED EXECUTION

═══════════════════════════════════════════════════════════════════
```

## Key Optimizations

| Optimization | Description | Example |
|-------------|-------------|---------|
| **Predicate Pushdown** | Filter early, read less data | Filter before join |
| **Column Pruning** | Only read needed columns | Skip unused columns |
| **Constant Folding** | Evaluate constants at compile time | `1 + 1` → `2` |
| **Join Reordering** | Put smaller tables first | Optimize join order |
| **Broadcast Join** | Broadcast small tables | Avoid shuffle |

## Predicate Pushdown Example

```
BEFORE OPTIMIZATION:
  SELECT * FROM orders
  JOIN customers ON orders.cust_id = customers.id
  WHERE customers.country = 'USA'

  Load ALL orders → Join → Filter (expensive!)


AFTER PREDICATE PUSHDOWN:
  SELECT * FROM orders
  JOIN (SELECT * FROM customers WHERE country = 'USA')
  ON orders.cust_id = customers.id

  Filter customers first → Join with smaller set (efficient!)
```

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-10-catalyst-optimizer && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~25 minutes

## Next
Continue to [kata-11-tungsten-engine](../kata-11-tungsten-engine/)
