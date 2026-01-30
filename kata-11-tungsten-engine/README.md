# Kata 11: Tungsten Engine

## Goal
Understand Spark's Tungsten execution engine and its optimizations for memory and CPU efficiency.
Learn about whole-stage code generation, UnsafeRow binary format, off-heap memory management,
and how Tungsten achieves near-native performance.

## What is Tungsten?

Project Tungsten is Spark's initiative to substantially improve the efficiency of memory and CPU
for Spark applications. It focuses on hardware-level optimizations that push performance closer
to the limits of modern hardware.

```
═══════════════════════════════════════════════════════════════════════════════════════
                          WHY TUNGSTEN WAS NEEDED
═══════════════════════════════════════════════════════════════════════════════════════

  TRADITIONAL JVM EXECUTION PROBLEMS:

  1. MEMORY OVERHEAD
     ┌─────────────────────────────────────────────────────────────────────────────┐
     │  Java String "Hello"                                                        │
     │  ┌──────────────────────────────────────────────────────────────────────┐  │
     │  │ Object Header  │ Hash Code │ Length │ Offset │ char[] reference │    │  │
     │  │   (12 bytes)   │ (4 bytes) │(4 bytes)│(4 bytes)│    (8 bytes)     │    │  │
     │  └──────────────────────────────────────────────────────────────────────┘  │
     │  Plus char[] array:                                                         │
     │  ┌──────────────────────────────────────────────────────────────────────┐  │
     │  │ Object Header  │ Length │ 'H' │ 'e' │ 'l' │ 'l' │ 'o' │ padding │    │  │
     │  │   (12 bytes)   │(4 bytes)│(2ea)│(2ea)│(2ea)│(2ea)│(2ea)│(6 bytes)│    │  │
     │  └──────────────────────────────────────────────────────────────────────┘  │
     │                                                                             │
     │  5-character string = 48+ bytes!  (vs 5 bytes actual data)                  │
     │  That's 10x overhead!                                                       │
     └─────────────────────────────────────────────────────────────────────────────┘

  2. GARBAGE COLLECTION PRESSURE
     ┌─────────────────────────────────────────────────────────────────────────────┐
     │  Processing 1 billion rows creates billions of objects                      │
     │  → Massive GC pauses (seconds to minutes!)                                  │
     │  → Unpredictable latency                                                    │
     │  → Wasted CPU cycles                                                        │
     └─────────────────────────────────────────────────────────────────────────────┘

  3. VIRTUAL FUNCTION CALLS
     ┌─────────────────────────────────────────────────────────────────────────────┐
     │  Traditional iterator model:                                                │
     │                                                                             │
     │  while (iterator.hasNext()) {           // Virtual call                     │
     │    val row = iterator.next()            // Virtual call                     │
     │    val value = row.getString(0)         // Virtual call                     │
     │    // ... process                                                           │
     │  }                                                                          │
     │                                                                             │
     │  Each virtual call = 10-20 CPU cycles overhead                              │
     │  Processing 1 billion rows = 30-60 billion wasted cycles!                   │
     └─────────────────────────────────────────────────────────────────────────────┘

  4. CPU CACHE MISSES
     ┌─────────────────────────────────────────────────────────────────────────────┐
     │  JVM objects scattered in heap                                              │
     │  ┌────┐     ┌────┐         ┌────┐    ┌────┐                                │
     │  │Row1│     │Row3│         │Row2│    │Row4│  ← Random memory locations     │
     │  └────┘     └────┘         └────┘    └────┘                                │
     │                                                                             │
     │  Sequential access pattern misses CPU cache → 100x slower                   │
     └─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Tungsten's Three Pillars

```
═══════════════════════════════════════════════════════════════════════════════════════
                        TUNGSTEN'S THREE PILLARS
═══════════════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  1. MEMORY MANAGEMENT                2. CODE GENERATION                         │
  │  ┌─────────────────────────┐        ┌─────────────────────────┐               │
  │  │ • Off-heap memory       │        │ • Whole-stage codegen   │               │
  │  │ • Binary data format    │        │ • Operator fusion       │               │
  │  │ • No GC pressure        │        │ • Loop unrolling        │               │
  │  │ • Compact storage       │        │ • SIMD vectorization    │               │
  │  └─────────────────────────┘        └─────────────────────────┘               │
  │                                                                                 │
  │  3. CACHE-AWARE COMPUTATION                                                     │
  │  ┌─────────────────────────────────────────────────────────────────┐           │
  │  │ • Sequential memory access patterns                             │           │
  │  │ • Cache-friendly data layouts                                   │           │
  │  │ • Exploits L1/L2/L3 CPU caches                                  │           │
  │  │ • Sort-based aggregation for large data                         │           │
  │  └─────────────────────────────────────────────────────────────────┘           │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## UnsafeRow: Tungsten's Binary Format

UnsafeRow is Tungsten's compact binary representation for rows. It eliminates JVM object overhead
and enables direct memory operations.

```
═══════════════════════════════════════════════════════════════════════════════════════
                              UnsafeRow FORMAT
═══════════════════════════════════════════════════════════════════════════════════════

  Consider this row: (123, "Hello", 45.67, null)
                      Int  String  Double  Int

  TRADITIONAL JVM OBJECTS:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  Row Object (48 bytes)                                                          │
  │  ┌──────────────────┬──────────────────────────────────────────────────────┐   │
  │  │ Object header    │ Array of field references                             │   │
  │  └──────────────────┴──────────────────────────────────────────────────────┘   │
  │           │                                                                     │
  │           ├──► Integer Object (16 bytes): 123                                   │
  │           ├──► String Object (48 bytes): "Hello"                                │
  │           ├──► Double Object (24 bytes): 45.67                                  │
  │           └──► null                                                             │
  │                                                                                 │
  │  Total: ~136 bytes + GC overhead                                                │
  └─────────────────────────────────────────────────────────────────────────────────┘

  UNSAFEROW BINARY FORMAT:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  ┌─────────────┬───────────────────────────────────┬───────────────────────┐   │
  │  │ Null Bitmap │ Fixed-Length Region               │ Variable-Length Region│   │
  │  │  (8 bytes)  │ (8 bytes per field)               │                       │   │
  │  └─────────────┴───────────────────────────────────┴───────────────────────┘   │
  │                                                                                 │
  │  DETAILED LAYOUT:                                                               │
  │  ┌──────────────────────────────────────────────────────────────────────────┐  │
  │  │ Offset 0-7   │ Null bitmap: 0b00001000 (bit 3 = null for field 4)       │  │
  │  ├──────────────┼──────────────────────────────────────────────────────────┤  │
  │  │ Offset 8-15  │ Field 1 (Int): 123 (stored directly as 8 bytes)          │  │
  │  ├──────────────┼──────────────────────────────────────────────────────────┤  │
  │  │ Offset 16-23 │ Field 2 (String): offset=32, length=5                    │  │
  │  ├──────────────┼──────────────────────────────────────────────────────────┤  │
  │  │ Offset 24-31 │ Field 3 (Double): 45.67 (64-bit IEEE 754)                │  │
  │  ├──────────────┼──────────────────────────────────────────────────────────┤  │
  │  │ Offset 32-36 │ "Hello" (5 bytes UTF-8)                                  │  │
  │  └──────────────┴──────────────────────────────────────────────────────────┘  │
  │                                                                                 │
  │  Total: 37 bytes (aligned to 40 bytes) - 3.4x smaller!                          │
  │  No GC! Direct memory access! Sequential layout!                                │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  NULL BITMAP EXPLANATION:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  8 bytes = 64 bits = can track null status for up to 64 fields                  │
  │                                                                                 │
  │  Bit positions:  0 1 2 3 4 5 6 7 ...                                            │
  │  Our row:        0 0 0 1 0 0 0 0     (1 at position 3 = field 4 is null)        │
  │                                                                                 │
  │  Check null:  (bitmap >> fieldIndex) & 1 == 1 → null                            │
  │               O(1) null check with bit manipulation!                            │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

### Fixed-Length vs Variable-Length Fields

```
═══════════════════════════════════════════════════════════════════════════════════════
                      FIXED vs VARIABLE LENGTH STORAGE
═══════════════════════════════════════════════════════════════════════════════════════

  FIXED-LENGTH TYPES (stored directly in fixed region):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  Type        │ Size    │ Storage                                               │
  │  ────────────┼─────────┼─────────────────────────────────────────────────────  │
  │  Boolean     │ 1 bit   │ Stored in 8 bytes (padded)                            │
  │  Byte        │ 1 byte  │ Stored in 8 bytes (padded)                            │
  │  Short       │ 2 bytes │ Stored in 8 bytes (padded)                            │
  │  Int         │ 4 bytes │ Stored in 8 bytes (padded)                            │
  │  Long        │ 8 bytes │ Stored directly                                       │
  │  Float       │ 4 bytes │ Stored in 8 bytes (padded)                            │
  │  Double      │ 8 bytes │ Stored directly                                       │
  │  Date        │ 4 bytes │ Days since epoch, stored in 8 bytes                   │
  │  Timestamp   │ 8 bytes │ Microseconds since epoch                              │
  └─────────────────────────────────────────────────────────────────────────────────┘

  VARIABLE-LENGTH TYPES (offset+length in fixed region, data in variable region):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  Type        │ Fixed Region          │ Variable Region                         │
  │  ────────────┼───────────────────────┼───────────────────────────────────────  │
  │  String      │ (offset, length)      │ UTF-8 bytes                             │
  │  Binary      │ (offset, length)      │ Raw bytes                               │
  │  Array       │ (offset, length)      │ ArrayData (num elements + data)         │
  │  Map         │ (offset, length)      │ Keys array + Values array               │
  │  Struct      │ (offset, length)      │ Nested UnsafeRow                        │
  │  Decimal     │ Depends on precision  │ High precision stored in variable       │
  └─────────────────────────────────────────────────────────────────────────────────┘

  OFFSET+LENGTH ENCODING (in 8 bytes):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  Bits 0-31:  offset from start of variable region                              │
  │  Bits 32-63: length in bytes                                                   │
  │                                                                                 │
  │  val encoded: Long = (length.toLong << 32) | offset                            │
  │  val offset: Int = encoded.toInt                                               │
  │  val length: Int = (encoded >> 32).toInt                                       │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Whole-Stage Code Generation

Whole-stage code generation fuses multiple operators into a single optimized function,
eliminating virtual function call overhead.

```
═══════════════════════════════════════════════════════════════════════════════════════
                      WHOLE-STAGE CODE GENERATION
═══════════════════════════════════════════════════════════════════════════════════════

  QUERY: SELECT name, age * 2 FROM people WHERE age > 25

  TRADITIONAL VOLCANO MODEL (without code generation):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  ┌─────────────┐                                                               │
  │  │  Project    │  while (child.hasNext()) {  // virtual call                   │
  │  │  name,age*2 │    row = child.next()       // virtual call                   │
  │  └──────┬──────┘    emit(row.get("name"), row.get("age") * 2) // virtual calls │
  │         │          }                                                            │
  │  ┌──────┴──────┐                                                               │
  │  │   Filter    │  while (child.hasNext()) {  // virtual call                   │
  │  │   age > 25  │    row = child.next()       // virtual call                   │
  │  └──────┬──────┘    if (row.get("age") > 25) // virtual call                   │
  │         │              emit(row)                                                │
  │  ┌──────┴──────┐  }                                                            │
  │  │    Scan     │  while (hasMoreRows()) {                                      │
  │  │   people    │    emit(nextRow())          // virtual call                   │
  │  └─────────────┘  }                                                            │
  │                                                                                 │
  │  Each row: 10+ virtual calls × 1 billion rows = SLOW!                          │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  TUNGSTEN WHOLE-STAGE CODEGEN:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  // All operators FUSED into one tight loop:                                    │
  │                                                                                 │
  │  while (scan.hasNext()) {                                                       │
  │    UnsafeRow row = scan.next();                                                │
  │    long age = row.getLong(1);         // Direct memory access                  │
  │    if (age > 25) {                    // No virtual call                       │
  │      outputRow.setString(0, row.getString(0));  // Direct copy                 │
  │      outputRow.setLong(1, age * 2);             // Inline computation          │
  │      emit(outputRow);                                                          │
  │    }                                                                            │
  │  }                                                                              │
  │                                                                                 │
  │  • No virtual calls between operators                                           │
  │  • Data stays in CPU registers                                                  │
  │  • JIT compiler can optimize the whole loop                                     │
  │  • 10x faster for many queries!                                                 │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

### Identifying Code Generation in Plans

```
═══════════════════════════════════════════════════════════════════════════════════════
                     CODE GENERATION IN EXPLAIN OUTPUT
═══════════════════════════════════════════════════════════════════════════════════════

  == Physical Plan ==
  *(1) Project [name#0, (age#1 * 2) AS doubled_age#5]        ← *(1) = WholeStageCodegen
  +- *(1) Filter (age#1 > 25)                                ← Same *(1) = fused together
     +- *(1) ColumnarToRow
        +- FileScan parquet [name#0,age#1]

  WHAT THE * MEANS:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  *(1)  = WholeStageCodegen stage 1                                              │
  │          All operations with *(1) are fused into ONE generated function         │
  │                                                                                 │
  │  *(2)  = WholeStageCodegen stage 2                                              │
  │          Separate stage (usually after a shuffle/exchange)                      │
  │                                                                                 │
  │  No *  = Operation not code-generated (UDF, complex expression)                 │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  EXAMPLE WITH MULTIPLE STAGES:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  *(2) HashAggregate(keys=[dept], functions=[sum(salary)])   ← Stage 2          │
  │  +- Exchange hashpartitioning(dept, 200)                    ← SHUFFLE (breaks) │
  │     +- *(1) HashAggregate(keys=[dept], functions=[partial_sum(salary)])        │
  │        +- *(1) Project [dept, salary]                       ← Stage 1          │
  │           +- *(1) Filter (age > 25)                                            │
  │              +- *(1) ColumnarToRow                                             │
  │                 +- FileScan parquet                                            │
  │                                                                                 │
  │  Stage 1: Scan → Filter → Project → Partial Aggregation (all fused)            │
  │  SHUFFLE: Data movement between executors (breaks codegen)                      │
  │  Stage 2: Final Aggregation                                                     │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

### Viewing Generated Code

```scala
// Enable debug mode to see generated code
spark.conf.set("spark.sql.codegen.wholeStage", "true")  // Default is true

// View generated code for a query
val df = spark.range(1000)
  .filter($"id" > 500)
  .select($"id" * 2)

df.queryExecution.debug.codegen()
// This prints the actual Java code that will be compiled and run!

// Alternative: use explain("codegen")
df.explain("codegen")
```

## Off-Heap Memory Management

Tungsten can store data outside the JVM heap, eliminating GC pressure.

```
═══════════════════════════════════════════════════════════════════════════════════════
                          MEMORY MANAGEMENT MODES
═══════════════════════════════════════════════════════════════════════════════════════

  ON-HEAP MODE (default for most operations):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  JVM Heap                                                                       │
  │  ┌─────────────────────────────────────────────────────────────────────────┐   │
  │  │                                                                         │   │
  │  │  byte[] arrays managed by Tungsten                                      │   │
  │  │  ┌──────────────────────────────────────────────────────────────────┐  │   │
  │  │  │ UnsafeRow │ UnsafeRow │ UnsafeRow │ UnsafeRow │ ...             │  │   │
  │  │  │  (binary) │  (binary) │  (binary) │  (binary) │                 │  │   │
  │  │  └──────────────────────────────────────────────────────────────────┘  │   │
  │  │                                                                         │   │
  │  │  GC still runs but:                                                     │   │
  │  │  • Few large arrays vs many small objects                               │   │
  │  │  • Much less GC work                                                    │   │
  │  │                                                                         │   │
  │  └─────────────────────────────────────────────────────────────────────────┘   │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  OFF-HEAP MODE (for sorting, shuffling):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  JVM Heap                           Native Memory (off-heap)                    │
  │  ┌─────────────────────┐           ┌─────────────────────────────────────────┐│
  │  │                     │           │                                         ││
  │  │  Pointers to        │ ───────►  │  Actual data stored here                ││
  │  │  off-heap memory    │           │  ┌────────────────────────────────────┐ ││
  │  │                     │           │  │ UnsafeRow │ UnsafeRow │ UnsafeRow │ ││
  │  │  (Minimal objects)  │           │  └────────────────────────────────────┘ ││
  │  │                     │           │                                         ││
  │  └─────────────────────┘           │  • No GC impact!                        ││
  │                                    │  • Direct memory access                 ││
  │                                    │  • Must be explicitly freed             ││
  │                                    └─────────────────────────────────────────┘│
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

### Memory Configuration

```scala
// Configure off-heap memory
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")

// Check current settings
spark.conf.get("spark.memory.offHeap.enabled")
spark.conf.get("spark.memory.offHeap.size")
```

## Cache-Aware Computation

Tungsten implements algorithms that are aware of CPU cache hierarchy.

```
═══════════════════════════════════════════════════════════════════════════════════════
                        CPU CACHE HIERARCHY
═══════════════════════════════════════════════════════════════════════════════════════

                    ┌─────────────────────────────────────────────────────────────┐
                    │                         CPU                                 │
                    │  ┌─────────────────────────────────────────────────────┐   │
                    │  │                    Core                              │   │
                    │  │  ┌─────────────────────────────────────────────┐    │   │
                    │  │  │         L1 Cache (32KB)                     │    │   │
                    │  │  │         Latency: ~4 cycles                  │    │   │
  Access Time:      │  │  └─────────────────────────────────────────────┘    │   │
                    │  │  ┌─────────────────────────────────────────────┐    │   │
  L1: 1ns           │  │  │         L2 Cache (256KB)                    │    │   │
  L2: 4ns           │  │  │         Latency: ~12 cycles                 │    │   │
  L3: 12ns          │  │  └─────────────────────────────────────────────┘    │   │
  RAM: 100ns        │  └─────────────────────────────────────────────────────┘   │
                    │                                                             │
                    │  ┌─────────────────────────────────────────────────────┐   │
                    │  │              L3 Cache (8-30MB)                       │   │
                    │  │              Latency: ~40 cycles                     │   │
                    │  │              Shared across cores                     │   │
                    │  └─────────────────────────────────────────────────────┘   │
                    └─────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │                    Main Memory (RAM)                        │
                    │                    Latency: ~200 cycles                     │
                    │                    Capacity: GBs                            │
                    └─────────────────────────────────────────────────────────────┘

  KEY INSIGHT: L1 cache hit is 100x faster than RAM access!

═══════════════════════════════════════════════════════════════════════════════════════
```

### Cache-Friendly Algorithms

```
═══════════════════════════════════════════════════════════════════════════════════════
                    CACHE-AWARE AGGREGATION EXAMPLE
═══════════════════════════════════════════════════════════════════════════════════════

  TRADITIONAL HASH AGGREGATION:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Hash Map (potentially huge, scattered in memory)                               │
  │  ┌────┐    ┌────┐        ┌────┐  ┌────┐       ┌────┐                          │
  │  │Key1│    │Key5│        │Key2│  │Key7│       │Key9│   ...                    │
  │  │Val │    │Val │        │Val │  │Val │       │Val │                          │
  │  └────┘    └────┘        └────┘  └────┘       └────┘                          │
  │                                                                                 │
  │  Problem: Random access pattern → constant cache misses                         │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  TUNGSTEN SORT-BASED AGGREGATION (when hash map too large):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Step 1: Store key-value pairs sequentially                                     │
  │  ┌──────────────────────────────────────────────────────────────────────────┐  │
  │  │ (Key1,Val) │ (Key2,Val) │ (Key1,Val) │ (Key3,Val) │ (Key2,Val) │ ...   │  │
  │  └──────────────────────────────────────────────────────────────────────────┘  │
  │                                                                                 │
  │  Step 2: Sort by key (cache-friendly merge sort)                                │
  │  ┌──────────────────────────────────────────────────────────────────────────┐  │
  │  │ (Key1,Val) │ (Key1,Val) │ (Key2,Val) │ (Key2,Val) │ (Key3,Val) │ ...   │  │
  │  └──────────────────────────────────────────────────────────────────────────┘  │
  │                                                                                 │
  │  Step 3: Sequential scan to aggregate same keys                                 │
  │  ┌──────────────────────────────────────────────────────────────────────────┐  │
  │  │ (Key1, SUM(Val)) │ (Key2, SUM(Val)) │ (Key3, SUM(Val)) │ ...            │  │
  │  └──────────────────────────────────────────────────────────────────────────┘  │
  │                                                                                 │
  │  Benefit: Sequential access → excellent cache utilization                       │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Vectorized Processing

Spark 3.x introduced vectorized processing for certain operations.

```
═══════════════════════════════════════════════════════════════════════════════════════
                        VECTORIZED PROCESSING
═══════════════════════════════════════════════════════════════════════════════════════

  ROW-AT-A-TIME PROCESSING:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  for each row in batch:                                                         │
  │    load row                    // Memory access                                 │
  │    compute result              // CPU operation                                 │
  │    store result                // Memory access                                 │
  │                                                                                 │
  │  Poor CPU utilization - instruction pipeline frequently stalled                 │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  VECTORIZED (COLUMNAR BATCH) PROCESSING:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Column Batch (1000+ values at a time):                                         │
  │                                                                                 │
  │  Age column: [25, 30, 35, 40, 25, 30, ...]                                      │
  │              ─────────────────────────────                                      │
  │                         │                                                       │
  │                         ▼                                                       │
  │              SIMD: Process 4-8 values in ONE CPU instruction!                   │
  │              [>25, >25, >25, >25] → [false, true, true, true]                   │
  │                                                                                 │
  │  Benefits:                                                                      │
  │  • CPU SIMD instructions (AVX2/AVX-512)                                         │
  │  • Better cache utilization                                                     │
  │  • Fewer instruction cache misses                                               │
  │  • 10x+ speedup for simple operations                                           │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

### Columnar Batch Format

```
═══════════════════════════════════════════════════════════════════════════════════════
                         COLUMNAR BATCH FORMAT
═══════════════════════════════════════════════════════════════════════════════════════

  COLUMNAR BATCH (ColumnarBatch):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  Batch of 1000 rows represented as column vectors:                              │
  │                                                                                 │
  │  ColumnVector "id":                                                             │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │ Null Bitmap: [0,0,0,1,0,0,0,0,...] (bit per row)                          │ │
  │  │ Values:      [1, 2, 3, _, 5, 6, 7, 8, ...]  (contiguous array)            │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  │  ColumnVector "name":                                                           │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │ Null Bitmap: [0,0,0,0,0,0,0,0,...]                                         │ │
  │  │ Offsets:     [0, 5, 8, 13, ...]                                            │ │
  │  │ Data:        "AliceBobCarolDave..."                                        │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  │  ColumnVector "age":                                                            │
  │  ┌───────────────────────────────────────────────────────────────────────────┐ │
  │  │ Null Bitmap: [0,0,0,0,0,0,0,0,...]                                         │ │
  │  │ Values:      [25, 30, 35, 40, 25, 30, 35, 40, ...]                         │ │
  │  └───────────────────────────────────────────────────────────────────────────┘ │
  │                                                                                 │
  │  PARQUET NATIVE: Read directly into column vectors (no deserialization!)       │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Configuration Options

```scala
// ============================================
// CODE GENERATION SETTINGS
// ============================================

// Enable/disable whole-stage code generation (default: true)
spark.conf.set("spark.sql.codegen.wholeStage", "true")

// Max fields for code generation (schemas larger than this won't use codegen)
spark.conf.set("spark.sql.codegen.maxFields", "100")

// Code generation fallback (fall back to interpreted mode on error)
spark.conf.set("spark.sql.codegen.fallback", "true")

// ============================================
// MEMORY SETTINGS
// ============================================

// Enable off-heap memory
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")

// ============================================
// VECTORIZED READER SETTINGS
// ============================================

// Enable vectorized Parquet reader (default: true)
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")

// Batch size for vectorized reader
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")

// ============================================
// DEBUGGING
// ============================================

// View generated code (for debugging)
df.queryExecution.debug.codegen()
```

## When Code Generation is Disabled

Certain operations prevent whole-stage code generation:

```
═══════════════════════════════════════════════════════════════════════════════════════
                    OPERATIONS THAT BREAK CODE GENERATION
═══════════════════════════════════════════════════════════════════════════════════════

  THESE PREVENT CODEGEN:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  1. User-Defined Functions (UDFs)                                               │
  │     val myUdf = udf((x: Int) => x * 2)                                          │
  │     df.select(myUdf($"col"))  // No codegen for UDF                            │
  │                                                                                 │
  │  2. Complex Types                                                               │
  │     df.select(explode($"array_col"))  // May disable codegen                   │
  │                                                                                 │
  │  3. Schemas with too many fields                                                │
  │     // > spark.sql.codegen.maxFields (default 100)                              │
  │                                                                                 │
  │  4. Python UDFs                                                                 │
  │     @udf("int")                                                                 │
  │     def my_python_udf(x): return x * 2  // Definitely no codegen               │
  │                                                                                 │
  │  5. Nested types exceeding depth limits                                         │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

  HOW TO CHECK:
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                 │
  │  df.explain()                                                                   │
  │                                                                                 │
  │  // Look for * prefix:                                                          │
  │  *(1) Filter ...  // WITH codegen                                              │
  │  Filter ...       // WITHOUT codegen (no star = bad!)                          │
  │                                                                                 │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Common Pitfalls and Solutions

### Pitfall 1: UDFs Killing Performance

```scala
// BAD: UDF prevents codegen and Tungsten optimization
val multiplyUdf = udf((x: Int) => x * 2)
df.select(multiplyUdf($"value"))  // 10x slower!

// GOOD: Use built-in functions
df.select($"value" * 2)  // Full Tungsten optimization

// GOOD: For complex logic, consider SQL expression
df.selectExpr("CASE WHEN value > 0 THEN value * 2 ELSE 0 END")
```

### Pitfall 2: Too Many Columns

```scala
// BAD: Schema with 200 columns exceeds codegen limit
val wideDF = spark.read.parquet("wide_table")  // 200 columns
wideDF.filter($"col1" > 100)  // No codegen!

// GOOD: Select only needed columns first
val narrowDF = wideDF.select("col1", "col2", "col3")  // 3 columns
narrowDF.filter($"col1" > 100)  // Codegen enabled!

// Or increase the limit (if needed)
spark.conf.set("spark.sql.codegen.maxFields", "200")
```

### Pitfall 3: Not Using Parquet/ORC

```scala
// SLOWER: CSV cannot use vectorized reader
val csvDF = spark.read.csv("data.csv")

// FASTER: Parquet uses vectorized reader + column pruning
val parquetDF = spark.read.parquet("data.parquet")

// Convert once, query many times
csvDF.write.parquet("data.parquet")
```

### Pitfall 4: Disabling Optimizations

```scala
// DON'T disable these without good reason:
spark.conf.set("spark.sql.codegen.wholeStage", "false")  // Major slowdown!
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")  // Slower reads
```

## Best Practices

### 1. Prefer Built-in Functions

```scala
import org.apache.spark.sql.functions._

// Use Spark's built-in functions - they're all Tungsten-optimized
df.select(
  upper($"name"),
  abs($"balance"),
  when($"status" === "active", 1).otherwise(0)
)
```

### 2. Use Columnar Formats

```scala
// Save as Parquet for best read performance
df.write
  .mode("overwrite")
  .parquet("/path/to/output")

// Enable dictionary encoding for low-cardinality columns
df.write
  .option("parquet.enable.dictionary", "true")
  .parquet("/path/to/output")
```

### 3. Monitor Codegen in Production

```scala
// Check if your queries use codegen
val plan = df.queryExecution.executedPlan
val usesCodegen = plan.toString.contains("WholeStageCodegen")
println(s"Uses code generation: $usesCodegen")

// Look for codegen stages in explain output
df.explain()
// *(1) = good, no * = investigate
```

### 4. Tune Batch Sizes

```scala
// For memory-constrained environments, reduce batch size
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "5000")

// For high-memory environments, increase for better throughput
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
```

### 5. Use Appropriate Data Types

```scala
// Use smallest type that fits your data
// Int (4 bytes) vs Long (8 bytes) matters at scale!

val schema = StructType(Seq(
  StructField("small_num", ShortType),   // -32K to 32K
  StructField("medium_num", IntegerType), // -2B to 2B
  StructField("big_num", LongType),      // Very large numbers
  StructField("precise", DecimalType(10, 2))  // Money
))
```

## Performance Comparison

```
═══════════════════════════════════════════════════════════════════════════════════════
                    TUNGSTEN PERFORMANCE IMPACT
═══════════════════════════════════════════════════════════════════════════════════════

  OPERATION: Filter + Project + Aggregate on 1 billion rows

  WITHOUT TUNGSTEN (RDD with Java objects):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  Memory Used:     ~100 GB (object overhead)                                     │
  │  GC Time:         ~30% of execution time                                        │
  │  Execution Time:  300 seconds                                                   │
  └─────────────────────────────────────────────────────────────────────────────────┘

  WITH TUNGSTEN (DataFrame with UnsafeRow):
  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │  Memory Used:     ~20 GB (binary format, no overhead)                           │
  │  GC Time:         ~2% of execution time                                         │
  │  Execution Time:  30 seconds (10x faster!)                                      │
  └─────────────────────────────────────────────────────────────────────────────────┘

  KEY FACTORS:
  • 5x less memory from binary format
  • 15x less GC time
  • 10x faster from code generation + cache-aware algorithms

═══════════════════════════════════════════════════════════════════════════════════════
```

## Instructions

1. **Read** this README thoroughly
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-11-tungsten-engine && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~45 minutes

## Next
Continue to [kata-12-joins-broadcast](../kata-12-joins-broadcast/)
