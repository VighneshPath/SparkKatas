import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner

/**
 * KATA 05: Partitioning
 *
 * Learn how data is distributed and how to control partitioning.
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 05: Partitioning")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata05")
    .master("local[4]")
    .getOrCreate()
  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Check default partition count
  // ─────────────────────────────────────────────────────────────────
  // Create an RDD and check how many partitions it has by default
  //
  // HINT: rdd.getNumPartitions
  // ─────────────────────────────────────────────────────────────────

  val defaultRDD = sc.parallelize(1 to 100)

  // TODO: Understand default partitioning is based on cluster configuration
  val defaultPartitions: Int = defaultRDD.getNumPartitions

  println(s"\n✓ Exercise 1: Default partitions")
  println(s"  Partitions: $defaultPartitions")
  println(s"  (Usually equals number of cores or default parallelism)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Create RDD with specific partition count
  // ─────────────────────────────────────────────────────────────────
  // Create an RDD with numbers 1-20 split into exactly 4 partitions
  //
  // HINT: sc.parallelize(data, numPartitions)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand how to control partition count at creation time
  val fourPartRDD = sc.parallelize(1 to 20, 4)

  println(s"\n✓ Exercise 2: Specific partition count")
  println(s"  Partitions: ${fourPartRDD.getNumPartitions}")
  assert(fourPartRDD.getNumPartitions == 4)
  println("  ✓ CORRECT!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: View partition contents with glom()
  // ─────────────────────────────────────────────────────────────────
  // Use glom() to see how elements are distributed across partitions
  // glom() transforms each partition into an array
  //
  // HINT: fourPartRDD.glom().collect()
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that glom() collects partition contents into arrays
  val partitionContents: Array[Array[Int]] = fourPartRDD.glom().collect()

  println(s"\n✓ Exercise 3: Partition contents")
  partitionContents.zipWithIndex.foreach { case (data, idx) =>
    println(s"  Partition $idx: [${data.mkString(", ")}]")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Repartition to increase partitions
  // ─────────────────────────────────────────────────────────────────
  // Increase fourPartRDD from 4 to 8 partitions
  // Note: repartition() causes a shuffle!
  //
  // HINT: rdd.repartition(8)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that repartition() causes a full shuffle
  val eightPartRDD = fourPartRDD.repartition(8)

  println(s"\n✓ Exercise 4: Repartition")
  println(s"  Original: ${fourPartRDD.getNumPartitions} partitions")
  println(s"  After repartition(8): ${eightPartRDD.getNumPartitions} partitions")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Coalesce to reduce partitions
  // ─────────────────────────────────────────────────────────────────
  // Reduce from 8 to 2 partitions using coalesce
  // coalesce() avoids shuffle when reducing partitions
  //
  // HINT: rdd.coalesce(2)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that coalesce() avoids shuffle when reducing partitions
  val twoPartRDD = eightPartRDD.coalesce(2)

  println(s"\n✓ Exercise 5: Coalesce")
  println(s"  After coalesce(2): ${twoPartRDD.getNumPartitions} partitions")
  println("  (No shuffle needed when reducing partitions!)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Hash Partitioning for Pair RDDs
  // ─────────────────────────────────────────────────────────────────
  // Create a pair RDD and partition by key using HashPartitioner
  // All elements with the same key will be in the same partition
  //
  // HINT: pairRDD.partitionBy(new HashPartitioner(3))
  // ─────────────────────────────────────────────────────────────────

  val pairRDD = sc.parallelize(List(
    ("a", 1), ("b", 2), ("c", 3),
    ("a", 4), ("b", 5), ("c", 6),
    ("a", 7), ("b", 8), ("c", 9)
  ))

  // TODO: Understand that HashPartitioner distributes keys evenly across partitions
  val hashPartitioned = pairRDD.partitionBy(new HashPartitioner(3))

  println(s"\n✓ Exercise 6: Hash Partitioning")
  println(s"  Partitioner: ${hashPartitioned.partitioner.getOrElse("None")}")

  hashPartitioned.glom().collect().zipWithIndex.foreach { case (data, idx) =>
    val keys = data.map(_._1).distinct.mkString(",")
    println(s"  Partition $idx: keys [$keys] -> ${data.mkString(", ")}")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Check if partitioner is preserved
  // ─────────────────────────────────────────────────────────────────
  // map() loses the partitioner, but mapValues() preserves it
  // Check the partitioner before and after transformation
  // ─────────────────────────────────────────────────────────────────

  val afterMap = hashPartitioned.map { case (k, v) => (k, v * 2) }
  val afterMapValues = hashPartitioned.mapValues(_ * 2)

  println(s"\n✓ Exercise 7: Partitioner preservation")
  println(s"  Original partitioner: ${hashPartitioned.partitioner.map(_.getClass.getSimpleName)}")
  println(s"  After map(): ${afterMap.partitioner.map(_.getClass.getSimpleName).getOrElse("None")}")
  println(s"  After mapValues(): ${afterMapValues.partitioner.map(_.getClass.getSimpleName).getOrElse("None")}")
  println("  (mapValues preserves partitioner, map does not!)")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 05 COMPLETE!")
  println("=" * 60)
}
