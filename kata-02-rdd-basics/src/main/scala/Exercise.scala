import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

/**
 * KATA 02: RDD Basics
 *
 * Learn about RDDs, partitions, and lineage.
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 02: RDD Basics")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata02")
    .master("local[4]")
    .getOrCreate()
  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Create an RDD from a collection
  // ─────────────────────────────────────────────────────────────────
  // Create an RDD containing numbers 1 to 20
  // HINT: sc.parallelize(1 to 20)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand how sc.parallelize creates an RDD
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 20)

  println(s"\n✓ Exercise 1: RDD created")
  println(s"  Element count: ${numbersRDD.count()}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Check partition count
  // ─────────────────────────────────────────────────────────────────
  // How many partitions does numbersRDD have?
  // HINT: .getNumPartitions
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand why partition count matters for parallelism
  val partitionCount: Int = numbersRDD.getNumPartitions

  println(s"\n✓ Exercise 2: Partition count")
  println(s"  Partitions: $partitionCount (default based on cores)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Create RDD with exactly 4 partitions
  // ─────────────────────────────────────────────────────────────────
  // Create an RDD with numbers 1-20 split into exactly 4 partitions
  // HINT: sc.parallelize(collection, numPartitions)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Try changing the partition count to 2 or 8 and observe
  val fourPartRDD: RDD[Int] = sc.parallelize(1 to 20, 4)

  println(s"\n✓ Exercise 3: RDD with 4 partitions")
  println(s"  Partitions: ${fourPartRDD.getNumPartitions}")
  assert(fourPartRDD.getNumPartitions == 4, "Should have 4 partitions")
  println("  ✓ CORRECT!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: View what's in each partition
  // ─────────────────────────────────────────────────────────────────
  // Use glom() to see how data is distributed across partitions
  // glom() collects each partition's elements into an array
  // HINT: fourPartRDD.glom().collect()
  // ─────────────────────────────────────────────────────────────────

  // TODO: Notice how data is distributed (roughly evenly)
  val partitionContents: Array[Array[Int]] = fourPartRDD.glom().collect()

  println(s"\n✓ Exercise 4: Partition contents")
  partitionContents.zipWithIndex.foreach { case (data, idx) =>
    println(s"  Partition $idx: [${data.mkString(", ")}]")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Create a Pair RDD (key-value)
  // ─────────────────────────────────────────────────────────────────
  // Transform fourPartRDD so each number becomes (isEven, number)
  // where isEven is true if number is even, false otherwise
  // HINT: .map(x => (x % 2 == 0, x))
  // ─────────────────────────────────────────────────────────────────

  // TODO: Pair RDDs enable groupByKey, reduceByKey operations
  val pairRDD: RDD[(Boolean, Int)] = fourPartRDD.map(x => (x % 2 == 0, x))

  println(s"\n✓ Exercise 5: Pair RDD")
  println(s"  Sample: ${pairRDD.take(6).mkString(", ")}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: View the lineage (debug string)
  // ─────────────────────────────────────────────────────────────────
  // The lineage shows how an RDD was derived
  // HINT: pairRDD.toDebugString
  // ─────────────────────────────────────────────────────────────────

  // TODO: Notice the chain of transformations in the lineage
  val lineage: String = pairRDD.toDebugString

  println(s"\n✓ Exercise 6: Lineage (shows RDD derivation)")
  lineage.split("\n").foreach(line => println(s"  $line"))

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Word count prep - split sentences into words
  // ─────────────────────────────────────────────────────────────────
  // Given sentences, create an RDD of individual words
  // HINT: Use flatMap to split and flatten
  //       sentencesRDD.flatMap(_.split(" "))
  // ─────────────────────────────────────────────────────────────────

  val sentencesRDD = sc.parallelize(List(
    "hello world",
    "spark is fast",
    "rdd is the foundation"
  ))

  // TODO: flatMap = map + flatten - perfect for splitting strings
  val wordsRDD: RDD[String] = sentencesRDD.flatMap(_.split(" "))

  println(s"\n✓ Exercise 7: Words RDD")
  println(s"  Words: ${wordsRDD.collect().mkString(", ")}")
  assert(wordsRDD.count() == 9, s"Expected 9 words, got ${wordsRDD.count()}")
  println("  ✓ CORRECT!")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 02 COMPLETE! Next: kata-03-transformations")
  println("=" * 60)
}
