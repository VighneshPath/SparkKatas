import org.apache.spark.sql.SparkSession

/**
 * KATA 07: Narrow vs Wide Transformations - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 07: Narrow vs Wide Transformations - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata07").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  // EXERCISE 1
  val numbers = sc.parallelize(1 to 100, 4)
  val narrowChain = numbers.map(_ * 2).filter(_ % 3 == 0).map(_ + 1)
  println(s"\n✓ Exercise 1: ${narrowChain.toDebugString}")

  // EXERCISE 2
  val withWide = narrowChain.repartition(2)
  println(s"\n✓ Exercise 2: ${withWide.toDebugString}")

  // EXERCISE 3
  val data = sc.parallelize(List("apple", "banana", "apple", "cherry", "banana"), 2)
  val mapped = data.map(s => (s, 1))
  val reduced = mapped.reduceByKey(_ + _)
  val doubled = reduced.mapValues(_ * 2)
  val sorted = doubled.sortByKey()

  val mapType = "narrow"
  val reduceByKeyType = "wide"
  val mapValuesType = "narrow"
  val sortByKeyType = "wide"

  println(s"\n✓ Exercise 3:")
  println(s"  map: $mapType")
  println(s"  reduceByKey: $reduceByKeyType")
  println(s"  mapValues: $mapValuesType")
  println(s"  sortByKey: $sortByKeyType")

  // EXERCISE 4
  println(s"\n✓ Exercise 4: ${sorted.toDebugString}")
  val numStages: Int = 3  // map->reduceByKey(shuffle)->mapValues->sortByKey(shuffle) = 3 stages
  println(s"  Number of stages: $numStages")

  // EXERCISE 5
  val rdd1 = sc.parallelize(List(("a", 1), ("b", 2)), 2)
  val rdd2 = sc.parallelize(List(("a", 3), ("c", 4)), 2)
  val unioned = rdd1.union(rdd2)
  val joinedResult = rdd1.join(rdd2)
  println(s"\n✓ Exercise 5:")
  println(s"  Union: ${unioned.collect().mkString(", ")}")
  println(s"  Join: ${joinedResult.collect().mkString(", ")}")

  // EXERCISE 6
  val nums = sc.parallelize(1 to 10, 2)
  val withMapPartitions = nums.mapPartitions(iter => iter.map(_ * 2))
  println(s"\n✓ Exercise 6: ${withMapPartitions.collect().mkString(", ")}")

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 07 SOLUTION COMPLETE!")
  println("=" * 60)
}
