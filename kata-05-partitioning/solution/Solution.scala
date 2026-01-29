import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner

/**
 * KATA 05: Partitioning - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 05: Partitioning - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata05").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  // EXERCISE 1
  val defaultRDD = sc.parallelize(1 to 100)
  val defaultPartitions: Int = defaultRDD.getNumPartitions
  println(s"\n✓ Exercise 1: $defaultPartitions partitions")

  // EXERCISE 2
  val fourPartRDD = sc.parallelize(1 to 20, 4)
  println(s"\n✓ Exercise 2: ${fourPartRDD.getNumPartitions} partitions")

  // EXERCISE 3
  val partitionContents: Array[Array[Int]] = fourPartRDD.glom().collect()
  println(s"\n✓ Exercise 3: Partition contents")
  partitionContents.zipWithIndex.foreach { case (data, idx) =>
    println(s"  Partition $idx: [${data.mkString(", ")}]")
  }

  // EXERCISE 4
  val eightPartRDD = fourPartRDD.repartition(8)
  println(s"\n✓ Exercise 4: ${eightPartRDD.getNumPartitions} partitions")

  // EXERCISE 5
  val twoPartRDD = eightPartRDD.coalesce(2)
  println(s"\n✓ Exercise 5: ${twoPartRDD.getNumPartitions} partitions")

  // EXERCISE 6
  val pairRDD = sc.parallelize(List(
    ("a", 1), ("b", 2), ("c", 3),
    ("a", 4), ("b", 5), ("c", 6),
    ("a", 7), ("b", 8), ("c", 9)
  ))
  val hashPartitioned = pairRDD.partitionBy(new HashPartitioner(3))
  println(s"\n✓ Exercise 6: ${hashPartitioned.partitioner}")
  hashPartitioned.glom().collect().zipWithIndex.foreach { case (data, idx) =>
    println(s"  Partition $idx: ${data.mkString(", ")}")
  }

  // EXERCISE 7
  val afterMap = hashPartitioned.map { case (k, v) => (k, v * 2) }
  val afterMapValues = hashPartitioned.mapValues(_ * 2)
  println(s"\n✓ Exercise 7:")
  println(s"  After map(): ${afterMap.partitioner}")
  println(s"  After mapValues(): ${afterMapValues.partitioner}")

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 05 SOLUTION COMPLETE!")
  println("=" * 60)
}
