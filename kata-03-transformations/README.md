# Kata 03: Transformations vs Actions

## Goal
Understand the two types of RDD operations and when computation actually happens.

## The Big Idea

```
╔═══════════════════════════════════════════════════════════════════╗
║                                                                    ║
║   TRANSFORMATIONS                    ACTIONS                       ║
║   ───────────────                    ───────                       ║
║   • Create new RDD                   • Return value to driver      ║
║   • LAZY (nothing happens)           • TRIGGER computation         ║
║   • Build the plan                   • Execute the plan            ║
║                                                                    ║
║   Examples:                          Examples:                     ║
║   map, filter, flatMap               collect, count, reduce        ║
║   groupByKey, reduceByKey            take, first, foreach          ║
║   join, union                        saveAsTextFile                ║
║                                                                    ║
╚═══════════════════════════════════════════════════════════════════╝
```

## Lazy Evaluation

```
  Transformations (just build the plan)      Action (execute!)
  ────────────────────────────────────       ─────────────────

  textFile ──► map ──► filter ──► map ──────► count()
     │          │        │         │              │
     └──────────┴────────┴─────────┘              │
              Nothing happens!                     │
                                            Everything runs!
```

## Common Transformations

| Transformation | Description | Example |
|---------------|-------------|---------|
| `map(f)` | Apply f to each element | `rdd.map(_ * 2)` |
| `filter(f)` | Keep elements where f is true | `rdd.filter(_ > 5)` |
| `flatMap(f)` | Map + flatten | `rdd.flatMap(_.split(" "))` |
| `distinct()` | Remove duplicates | `rdd.distinct()` |
| `reduceByKey(f)` | Aggregate by key | `rdd.reduceByKey(_ + _)` |

## Common Actions

| Action | Description | Example |
|--------|-------------|---------|
| `collect()` | Return all elements | `rdd.collect()` |
| `count()` | Count elements | `rdd.count()` |
| `take(n)` | Return first n | `rdd.take(5)` |
| `reduce(f)` | Aggregate elements | `rdd.reduce(_ + _)` |
| `first()` | Return first element | `rdd.first()` |

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-03-transformations && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~20 minutes

## Next
After completing this kata, proceed to `kata-04-lazy-evaluation`
