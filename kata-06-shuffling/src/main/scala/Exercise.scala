import org.apache.spark.sql.SparkSession

/**
 * KATA 06: Shuffling
 *
 * Learn what shuffles are and how to minimize them.
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 06: Shuffling")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata06")
    .master("local[4]")
    .getOrCreate()
  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Observe a shuffle with groupByKey
  // ─────────────────────────────────────────────────────────────────
  // Create a pair RDD and use groupByKey to group values by key
  // Then check the toDebugString to see the shuffle
  //
  // HINT: pairRDD.groupByKey()
  // ─────────────────────────────────────────────────────────────────

  val words = sc.parallelize(List(
    "apple", "banana", "apple", "cherry", "banana", "apple",
    "date", "cherry", "banana", "apple"
  ), 4)

  // Create pair RDD: (word, 1)
  val wordPairs = words.map(word => (word, 1))

  // TODO: Understand that groupByKey() causes a shuffle to group values by key
  val grouped = wordPairs.groupByKey()

  println(s"\n✓ Exercise 1: groupByKey")
  println(s"  Lineage (look for 'ShuffledRDD'):")
  println(s"  ${grouped.toDebugString}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Compare groupByKey vs reduceByKey
  // ─────────────────────────────────────────────────────────────────
  // Both achieve word count, but reduceByKey is more efficient
  // It reduces locally before shuffling
  //
  // HINT: pairRDD.reduceByKey(_ + _)
  // ─────────────────────────────────────────────────────────────────

  // Approach 1: groupByKey then sum (inefficient)
  val countWithGroup = grouped.mapValues(_.sum)

  // TODO: Understand that reduceByKey combines locally before shuffling
  val countWithReduce = wordPairs.reduceByKey(_ + _)

  println(s"\n✓ Exercise 2: groupByKey vs reduceByKey")
  println(s"  groupByKey + mapValues result: ${countWithGroup.collect().mkString(", ")}")
  println(s"  reduceByKey result: ${countWithReduce.collect().mkString(", ")}")
  println(s"  (Both give same result, but reduceByKey shuffles less data!)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Check shuffle read/write with toDebugString
  // ─────────────────────────────────────────────────────────────────
  // Look at the lineage to understand the shuffle stages
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 3: Lineage comparison")
  println(s"  reduceByKey lineage:")
  println(s"  ${countWithReduce.toDebugString}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Avoid shuffle with mapValues
  // ─────────────────────────────────────────────────────────────────
  // After a shuffle, using map() loses the partitioner
  // Using mapValues() preserves it (no extra shuffle needed)
  //
  // Check if partitioner is preserved
  // ─────────────────────────────────────────────────────────────────

  val partitioned = wordPairs.reduceByKey(_ + _)

  val afterMap = partitioned.map { case (k, v) => (k, v * 2) }
  val afterMapValues = partitioned.mapValues(_ * 2)

  println(s"\n✓ Exercise 4: Partitioner preservation")
  println(s"  After reduceByKey: ${partitioned.partitioner}")
  println(s"  After map(): ${afterMap.partitioner}")
  println(s"  After mapValues(): ${afterMapValues.partitioner}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Join causes shuffle
  // ─────────────────────────────────────────────────────────────────
  // Joining two RDDs requires bringing matching keys together
  //
  // HINT: rdd1.join(rdd2)
  // ─────────────────────────────────────────────────────────────────

  val prices = sc.parallelize(List(
    ("apple", 1.0), ("banana", 0.5), ("cherry", 2.0)
  ))

  val quantities = sc.parallelize(List(
    ("apple", 10), ("banana", 20), ("cherry", 5)
  ))

  // TODO: Understand that join() requires shuffling both RDDs by key
  val joined = prices.join(quantities)

  println(s"\n✓ Exercise 5: Join shuffle")
  println(s"  Joined result: ${joined.collect().mkString(", ")}")
  println(s"  Lineage (notice ShuffledRDD for both inputs):")
  println(s"  ${joined.toDebugString}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Coalesce vs Repartition
  // ─────────────────────────────────────────────────────────────────
  // repartition() always shuffles
  // coalesce() avoids shuffle when reducing partitions
  //
  // Create an RDD with 8 partitions, then reduce to 2
  // ─────────────────────────────────────────────────────────────────

  val eightParts = sc.parallelize(1 to 100, 8)

  // Reduce to 2 partitions - which is more efficient?
  val withRepartition = eightParts.repartition(2)
  val withCoalesce = eightParts.coalesce(2)

  println(s"\n✓ Exercise 6: Coalesce vs Repartition")
  println(s"  Original: ${eightParts.getNumPartitions} partitions")
  println(s"  repartition(2): ${withRepartition.getNumPartitions} partitions")
  println(s"  coalesce(2): ${withCoalesce.getNumPartitions} partitions")
  println(s"\n  Repartition lineage (has shuffle):")
  println(s"  ${withRepartition.toDebugString}")
  println(s"\n  Coalesce lineage (no shuffle):")
  println(s"  ${withCoalesce.toDebugString}")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 06 COMPLETE!")
  println("=" * 60)
}
