import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * KATA 08: Caching and Persistence
 *
 * Learn when and how to cache RDDs to avoid recomputation.
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 08: Caching and Persistence")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata08")
    .master("local[4]")
    .getOrCreate()
  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Observe recomputation without caching
  // ─────────────────────────────────────────────────────────────────
  // Create an RDD with an "expensive" transformation
  // Call multiple actions and observe recomputation
  // ─────────────────────────────────────────────────────────────────

  var computeCount = 0

  val expensiveRDD = sc.parallelize(1 to 10, 2).map { x =>
    computeCount += 1  // Track how many times this runs
    Thread.sleep(10)   // Simulate expensive computation
    x * x
  }

  println(s"\n✓ Exercise 1: Without caching")
  computeCount = 0

  // First action
  val sum1 = expensiveRDD.reduce(_ + _)
  println(s"  First action (reduce): sum = $sum1")
  println(s"  Compute count after first action: $computeCount")

  // Second action - recomputes!
  val count1 = expensiveRDD.count()
  println(s"  Second action (count): count = $count1")
  println(s"  Compute count after second action: $computeCount")
  println(s"  (Notice: computed ${computeCount} times total - recomputed!)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Use cache() to avoid recomputation
  // ─────────────────────────────────────────────────────────────────
  // Call .cache() on the RDD before the first action
  //
  // HINT: rdd.cache()
  // ─────────────────────────────────────────────────────────────────

  computeCount = 0

  val cachedRDD = sc.parallelize(1 to 10, 2).map { x =>
    computeCount += 1
    Thread.sleep(10)
    x * x
  }

  // TODO: Understand that cache() stores RDD in memory for reuse
  cachedRDD.cache()

  println(s"\n✓ Exercise 2: With caching")

  // First action - computes and caches
  val sum2 = cachedRDD.reduce(_ + _)
  println(s"  First action (reduce): sum = $sum2")
  println(s"  Compute count after first action: $computeCount")

  // Second action - uses cache!
  val count2 = cachedRDD.count()
  println(s"  Second action (count): count = $count2")
  println(s"  Compute count after second action: $computeCount")
  println(s"  (With cache: computed only ${computeCount} times!)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Check storage level
  // ─────────────────────────────────────────────────────────────────
  // cache() uses MEMORY_ONLY by default
  // Check the storage level with .getStorageLevel
  //
  // HINT: rdd.getStorageLevel
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand how to check the storage level of a cached RDD
  val storageLevel = cachedRDD.getStorageLevel

  println(s"\n✓ Exercise 3: Storage level")
  println(s"  Storage level: $storageLevel")
  println(s"  Uses memory: ${storageLevel.useMemory}")
  println(s"  Uses disk: ${storageLevel.useDisk}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Use persist() with custom storage level
  // ─────────────────────────────────────────────────────────────────
  // persist() allows specifying a storage level
  //
  // HINT: rdd.persist(StorageLevel.MEMORY_AND_DISK)
  // ─────────────────────────────────────────────────────────────────

  val memAndDiskRDD = sc.parallelize(1 to 100, 4)

  // TODO: Understand different storage levels (MEMORY_ONLY, MEMORY_AND_DISK, etc.)
  memAndDiskRDD.persist(StorageLevel.MEMORY_AND_DISK)

  memAndDiskRDD.count() // Trigger caching

  println(s"\n✓ Exercise 4: Custom storage level")
  println(s"  Storage level: ${memAndDiskRDD.getStorageLevel}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Unpersist to free memory
  // ─────────────────────────────────────────────────────────────────
  // Remove cached data when no longer needed
  //
  // HINT: rdd.unpersist()
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand the importance of unpersisting to free resources
  cachedRDD.unpersist()
  memAndDiskRDD.unpersist()

  println(s"\n✓ Exercise 5: Unpersist")
  println(s"  cachedRDD storage after unpersist: ${cachedRDD.getStorageLevel}")
  println(s"  (StorageLevel.NONE means not cached)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Cache at branch points
  // ─────────────────────────────────────────────────────────────────
  // When one RDD feeds multiple downstream RDDs, cache it
  // ─────────────────────────────────────────────────────────────────

  val baseRDD = sc.parallelize(1 to 1000, 4)
    .map(_ * 2)
    .filter(_ % 3 == 0)

  // This RDD is used multiple times - should cache!
  baseRDD.cache()

  // Multiple downstream computations
  val sumResult = baseRDD.reduce(_ + _)
  val maxResult = baseRDD.max()
  val countResult = baseRDD.count()

  println(s"\n✓ Exercise 6: Cache at branch points")
  println(s"  Sum: $sumResult")
  println(s"  Max: $maxResult")
  println(s"  Count: $countResult")
  println(s"  (baseRDD was computed once and reused 3 times)")

  baseRDD.unpersist()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Serialized caching
  // ─────────────────────────────────────────────────────────────────
  // MEMORY_ONLY_SER stores serialized objects (more compact)
  // Good when memory is tight
  // ─────────────────────────────────────────────────────────────────

  val largeRDD = sc.parallelize(1 to 10000, 4)
    .map(x => (x, "value_" + x))

  largeRDD.persist(StorageLevel.MEMORY_ONLY_SER)
  largeRDD.count()

  println(s"\n✓ Exercise 7: Serialized caching")
  println(s"  Storage level: ${largeRDD.getStorageLevel}")
  println(s"  Deserialized: ${largeRDD.getStorageLevel.deserialized}")
  println(s"  (false = serialized, more memory efficient)")

  largeRDD.unpersist()

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 08 COMPLETE!")
  println("=" * 60)
}
