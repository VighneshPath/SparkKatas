import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

/**
 * KATA 02: RDD Basics - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 02: RDD Basics - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata02")
    .master("local[4]")
    .getOrCreate()
  val sc = spark.sparkContext

  // EXERCISE 1
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 20)
  println(s"\n✓ Exercise 1: Element count: ${numbersRDD.count()}")

  // EXERCISE 2
  val partitionCount: Int = numbersRDD.getNumPartitions
  println(s"\n✓ Exercise 2: Partitions: $partitionCount")

  // EXERCISE 3
  val fourPartRDD: RDD[Int] = sc.parallelize(1 to 20, 4)
  println(s"\n✓ Exercise 3: Partitions: ${fourPartRDD.getNumPartitions}")

  // EXERCISE 4
  val partitionContents: Array[Array[Int]] = fourPartRDD.glom().collect()
  println(s"\n✓ Exercise 4: Partition contents")
  partitionContents.zipWithIndex.foreach { case (data, idx) =>
    println(s"  Partition $idx: [${data.mkString(", ")}]")
  }

  // EXERCISE 5
  val pairRDD: RDD[(Boolean, Int)] = fourPartRDD.map(x => (x % 2 == 0, x))
  println(s"\n✓ Exercise 5: ${pairRDD.take(6).mkString(", ")}")

  // EXERCISE 6
  val lineage: String = pairRDD.toDebugString
  println(s"\n✓ Exercise 6: Lineage")
  lineage.split("\n").foreach(line => println(s"  $line"))

  // EXERCISE 7
  val sentencesRDD = sc.parallelize(List("hello world", "spark is fast", "rdd is the foundation"))
  val wordsRDD: RDD[String] = sentencesRDD.flatMap(_.split(" "))
  println(s"\n✓ Exercise 7: ${wordsRDD.collect().mkString(", ")}")

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 02 SOLUTION COMPLETE!")
  println("=" * 60)
}
