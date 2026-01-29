import org.apache.spark.sql.SparkSession

/**
 * KATA 06: Shuffling - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 06: Shuffling - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata06").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  // EXERCISE 1
  val words = sc.parallelize(List(
    "apple", "banana", "apple", "cherry", "banana", "apple",
    "date", "cherry", "banana", "apple"
  ), 4)
  val wordPairs = words.map(word => (word, 1))
  val grouped = wordPairs.groupByKey()
  println(s"\n✓ Exercise 1: groupByKey")
  println(s"  Lineage: ${grouped.toDebugString}")

  // EXERCISE 2
  val countWithGroup = grouped.mapValues(_.sum)
  val countWithReduce = wordPairs.reduceByKey(_ + _)
  println(s"\n✓ Exercise 2: groupByKey vs reduceByKey")
  println(s"  groupByKey result: ${countWithGroup.collect().mkString(", ")}")
  println(s"  reduceByKey result: ${countWithReduce.collect().mkString(", ")}")

  // EXERCISE 3
  println(s"\n✓ Exercise 3: Lineage comparison")
  println(s"  ${countWithReduce.toDebugString}")

  // EXERCISE 4
  val partitioned = wordPairs.reduceByKey(_ + _)
  val afterMap = partitioned.map { case (k, v) => (k, v * 2) }
  val afterMapValues = partitioned.mapValues(_ * 2)
  println(s"\n✓ Exercise 4: Partitioner preservation")
  println(s"  After reduceByKey: ${partitioned.partitioner}")
  println(s"  After map(): ${afterMap.partitioner}")
  println(s"  After mapValues(): ${afterMapValues.partitioner}")

  // EXERCISE 5
  val prices = sc.parallelize(List(("apple", 1.0), ("banana", 0.5), ("cherry", 2.0)))
  val quantities = sc.parallelize(List(("apple", 10), ("banana", 20), ("cherry", 5)))
  val joined = prices.join(quantities)
  println(s"\n✓ Exercise 5: Join shuffle")
  println(s"  Joined: ${joined.collect().mkString(", ")}")
  println(s"  Lineage: ${joined.toDebugString}")

  // EXERCISE 6
  val eightParts = sc.parallelize(1 to 100, 8)
  val withRepartition = eightParts.repartition(2)
  val withCoalesce = eightParts.coalesce(2)
  println(s"\n✓ Exercise 6: Coalesce vs Repartition")
  println(s"  Repartition lineage: ${withRepartition.toDebugString}")
  println(s"  Coalesce lineage: ${withCoalesce.toDebugString}")

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 06 SOLUTION COMPLETE!")
  println("=" * 60)
}
