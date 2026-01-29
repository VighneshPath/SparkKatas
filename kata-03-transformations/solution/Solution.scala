import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

/**
 * KATA 03: Transformations vs Actions - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 03: Transformations vs Actions - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata03").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  val numbers = sc.parallelize(1 to 20)

  // EXERCISE 1: map
  val doubled: RDD[Int] = numbers.map(_ * 2)
  println(s"\n✓ Exercise 1: ${doubled.take(5).mkString(", ")}")

  // EXERCISE 2: filter
  val evens: RDD[Int] = numbers.filter(_ % 2 == 0)
  println(s"\n✓ Exercise 2: ${evens.collect().mkString(", ")}")

  // EXERCISE 3: flatMap
  val expanded: RDD[Int] = sc.parallelize(1 to 3).flatMap(n => 1 to n)
  println(s"\n✓ Exercise 3: ${expanded.collect().mkString(", ")}")

  // EXERCISE 4: reduce
  val sum: Int = numbers.reduce(_ + _)
  println(s"\n✓ Exercise 4: Sum = $sum")

  // EXERCISE 5: reduceByKey
  val words = sc.parallelize(List("apple", "banana", "apple", "cherry", "banana", "apple"))
  val wordCounts: RDD[(String, Int)] = words.map(w => (w, 1)).reduceByKey(_ + _)
  println(s"\n✓ Exercise 5:")
  wordCounts.collect().sortBy(_._1).foreach { case (w, c) => println(s"  $w: $c") }

  // EXERCISE 6: Chaining
  val result: Array[Int] = sc.parallelize(1 to 10).map(_ * 2).filter(_ > 10).collect()
  println(s"\n✓ Exercise 6: ${result.mkString(", ")}")

  // EXERCISE 7: distinct
  val withDupes = sc.parallelize(List(1, 2, 2, 3, 3, 3, 4, 4, 4, 4))
  val unique: RDD[Int] = withDupes.distinct()
  println(s"\n✓ Exercise 7: ${unique.collect().sorted.mkString(", ")}")

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 03 SOLUTION COMPLETE!")
  println("=" * 60)
}
