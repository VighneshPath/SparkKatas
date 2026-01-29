# Kata 08: Caching and Persistence

## Goal
Learn when and how to cache RDDs to avoid recomputation.

## Why Cache?

```
═══════════════════════════════════════════════════════════════════
                    WITHOUT CACHING
═══════════════════════════════════════════════════════════════════

  val rdd = sc.textFile("data.txt")
                 .map(expensive)
                 .filter(complex)

  rdd.count()    // Computes entire pipeline
  rdd.collect()  // Computes AGAIN from scratch!

  Each action recomputes everything!

═══════════════════════════════════════════════════════════════════
                     WITH CACHING
═══════════════════════════════════════════════════════════════════

  val rdd = sc.textFile("data.txt")
                 .map(expensive)
                 .filter(complex)
                 .cache()   // Mark for caching

  rdd.count()    // Computes and STORES in memory
  rdd.collect()  // Uses cached data - FAST!

  First action caches, subsequent actions reuse!

═══════════════════════════════════════════════════════════════════
```

## Storage Levels

| Level | Memory | Disk | Serialized | Replicated |
|-------|--------|------|------------|------------|
| `MEMORY_ONLY` | Yes | No | No | No |
| `MEMORY_AND_DISK` | Yes | Yes | No | No |
| `MEMORY_ONLY_SER` | Yes | No | Yes | No |
| `MEMORY_AND_DISK_SER` | Yes | Yes | Yes | No |
| `DISK_ONLY` | No | Yes | Yes | No |
| `MEMORY_ONLY_2` | Yes | No | No | 2x |

```
CHOOSING STORAGE LEVEL:
═══════════════════════════════════════════════════════════════════

  MEMORY_ONLY (default for cache())
    ✓ Fastest access
    ✗ May not fit in memory
    Use when: RDD fits comfortably in memory

  MEMORY_AND_DISK
    ✓ Won't lose data if memory fills
    ✗ Slower disk access
    Use when: RDD too big for memory

  MEMORY_ONLY_SER
    ✓ More space efficient (2-5x)
    ✗ CPU cost to deserialize
    Use when: Memory constrained

  DISK_ONLY
    ✓ Handles any size
    ✗ Slowest
    Use when: Recomputation is very expensive

═══════════════════════════════════════════════════════════════════
```

## When to Cache

```
✓ CACHE WHEN:
  - RDD is used multiple times
  - RDD is expensive to compute
  - RDD is at a branch point (used by multiple downstream RDDs)

✗ DON'T CACHE WHEN:
  - RDD is used only once
  - RDD is cheap to compute
  - Memory is limited and RDD is large
```

## Instructions

1. **Read** this README
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-08-caching-persistence && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~20 minutes

## Next
Continue to [kata-09-dataframes](../kata-09-dataframes/)
