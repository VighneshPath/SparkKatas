import org.apache.spark.sql.SparkSession

/**
 * KATA 07: Narrow vs Wide Transformations
 *
 * Learn the difference between narrow and wide transformations.
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 07: Narrow vs Wide Transformations")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata07")
    .master("local[4]")
    .getOrCreate()
  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Chain narrow transformations
  // ─────────────────────────────────────────────────────────────────
  // Narrow transformations can be pipelined - they stay in one stage
  //
  // Chain: map → filter → map
  // Check the lineage - all in one stage!
  // ─────────────────────────────────────────────────────────────────

  val numbers = sc.parallelize(1 to 100, 4)

  val narrowChain = numbers
    .map(_ * 2)          // narrow
    .filter(_ % 3 == 0)  // narrow
    .map(_ + 1)          // narrow

  println(s"\n✓ Exercise 1: Narrow transformation chain")
  println(s"  Lineage (all in one stage, no shuffle):")
  println(s"  ${narrowChain.toDebugString}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Wide transformation creates stage boundary
  // ─────────────────────────────────────────────────────────────────
  // Adding a wide transformation splits the execution into stages
  //
  // HINT: Use repartition() to add a wide transformation
  // ─────────────────────────────────────────────────────────────────

  val withWide = narrowChain.repartition(2)  // wide - creates new stage

  println(s"\n✓ Exercise 2: Wide transformation stage boundary")
  println(s"  Lineage (notice the stage boundary at ShuffledRDD):")
  println(s"  ${withWide.toDebugString}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Identify narrow vs wide
  // ─────────────────────────────────────────────────────────────────
  // Examine each transformation and determine if narrow or wide
  // ─────────────────────────────────────────────────────────────────

  val data = sc.parallelize(List("apple", "banana", "apple", "cherry", "banana"), 2)

  // Transformation 1: map (narrow or wide?)
  val mapped = data.map(s => (s, 1))

  // Transformation 2: reduceByKey (narrow or wide?)
  val reduced = mapped.reduceByKey(_ + _)

  // Transformation 3: mapValues (narrow or wide?)
  val doubled = reduced.mapValues(_ * 2)

  // Transformation 4: sortByKey (narrow or wide?)
  val sorted = doubled.sortByKey()

  println(s"\n✓ Exercise 3: Identify transformation types")
  println(s"  Complete the answers below:")

  // TODO: Understand the difference between narrow and wide transformations
  val mapType = "narrow"         // map() processes each element independently
  val reduceByKeyType = "wide"   // reduceByKey() requires shuffling data by key
  val mapValuesType = "narrow"   // mapValues() preserves partitioning
  val sortByKeyType = "wide"     // sortByKey() requires shuffling for global sort

  println(s"  map: $mapType")
  println(s"  reduceByKey: $reduceByKeyType")
  println(s"  mapValues: $mapValuesType")
  println(s"  sortByKey: $sortByKeyType")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Count stages in lineage
  // ─────────────────────────────────────────────────────────────────
  // Each shuffle creates a new stage
  // Look at the indentation in toDebugString
  //
  // HINT: Count the number of ShuffledRDD + 1
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 4: Count stages")
  println(s"  Full lineage:")
  println(s"  ${sorted.toDebugString}")

  // TODO: Understand how to count stages from the lineage
  val numStages: Int = 3  // Initial stage + reduceByKey shuffle + sortByKey shuffle
  println(s"  Number of stages: $numStages")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: union is narrow, join is wide
  // ─────────────────────────────────────────────────────────────────
  // union combines RDDs without moving data
  // join requires bringing matching keys together
  // ─────────────────────────────────────────────────────────────────

  val rdd1 = sc.parallelize(List(("a", 1), ("b", 2)), 2)
  val rdd2 = sc.parallelize(List(("a", 3), ("c", 4)), 2)

  // Union - narrow (no shuffle)
  val unioned = rdd1.union(rdd2)

  // TODO: Understand that join() is a wide transformation requiring shuffle
  val joinedResult = rdd1.join(rdd2)

  println(s"\n✓ Exercise 5: union vs join")
  println(s"  Union result: ${unioned.collect().mkString(", ")}")
  println(s"  Union lineage (narrow - no shuffle):")
  println(s"  ${unioned.toDebugString}")
  println(s"\n  Join result: ${joinedResult.collect().mkString(", ")}")
  println(s"  Join lineage (wide - has shuffle):")
  println(s"  ${joinedResult.toDebugString}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: mapPartitions is narrow
  // ─────────────────────────────────────────────────────────────────
  // mapPartitions processes entire partition at once
  // Still narrow - no data movement between partitions
  //
  // HINT: mapPartitions(iter => iter.map(_ * 2))
  // ─────────────────────────────────────────────────────────────────

  val nums = sc.parallelize(1 to 10, 2)

  // TODO: Understand that mapPartitions is narrow (processes partition locally)
  val withMapPartitions = nums.mapPartitions(iter => iter.map(_ * 2))

  println(s"\n✓ Exercise 6: mapPartitions (narrow)")
  println(s"  Result: ${withMapPartitions.collect().mkString(", ")}")
  println(s"  Lineage (no shuffle):")
  println(s"  ${withMapPartitions.toDebugString}")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 07 COMPLETE!")
  println("=" * 60)
}
