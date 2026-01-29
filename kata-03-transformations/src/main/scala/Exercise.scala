import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

/**
 * KATA 03: Transformations vs Actions
 *
 * Learn the difference between transformations (lazy) and actions (trigger execution).
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 03: Transformations vs Actions")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata03")
    .master("local[4]")
    .getOrCreate()
  val sc = spark.sparkContext

  val numbers = sc.parallelize(1 to 20)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: map - transform each element
  // ─────────────────────────────────────────────────────────────────
  // Double each number using map
  //
  // HINT: numbers.map(x => x * 2) or numbers.map(_ * 2)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that map() is a transformation that applies a function to each element
  val doubled: RDD[Int] = numbers.map(_ * 2)

  println(s"\n✓ Exercise 1: map")
  println(s"  First 5 doubled: ${doubled.take(5).mkString(", ")}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: filter - keep matching elements
  // ─────────────────────────────────────────────────────────────────
  // Keep only even numbers from the original numbers RDD
  //
  // HINT: numbers.filter(x => x % 2 == 0)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that filter() keeps elements that satisfy the predicate
  val evens: RDD[Int] = numbers.filter(_ % 2 == 0)

  println(s"\n✓ Exercise 2: filter")
  println(s"  Even numbers: ${evens.collect().mkString(", ")}")
  assert(evens.count() == 10, "Should have 10 even numbers")
  println("  ✓ CORRECT!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: flatMap - map + flatten
  // ─────────────────────────────────────────────────────────────────
  // For each number 1-3, generate a sequence from 1 to that number
  // e.g., 1 -> [1], 2 -> [1,2], 3 -> [1,2,3]
  // Result should be: [1, 1, 2, 1, 2, 3]
  //
  // HINT: sc.parallelize(1 to 3).flatMap(n => 1 to n)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that flatMap maps then flattens the results
  val expanded: RDD[Int] = sc.parallelize(1 to 3).flatMap(n => 1 to n)

  println(s"\n✓ Exercise 3: flatMap")
  println(s"  Expanded: ${expanded.collect().mkString(", ")}")
  assert(expanded.count() == 6, "Should have 6 elements")
  println("  ✓ CORRECT!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: reduce - aggregate all elements
  // ─────────────────────────────────────────────────────────────────
  // Calculate the sum of numbers 1-20 using reduce
  //
  // HINT: numbers.reduce((a, b) => a + b) or numbers.reduce(_ + _)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that reduce() is an ACTION that aggregates elements
  val sum: Int = numbers.reduce(_ + _)

  println(s"\n✓ Exercise 4: reduce")
  println(s"  Sum of 1-20: $sum")
  assert(sum == 210, s"Expected 210, got $sum")
  println("  ✓ CORRECT!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Word count with reduceByKey
  // ─────────────────────────────────────────────────────────────────
  // Count word occurrences in the given words
  // Steps:
  //   1. Map each word to (word, 1)
  //   2. Use reduceByKey to sum the counts
  //
  // HINT: words.map(w => (w, 1)).reduceByKey(_ + _)
  // ─────────────────────────────────────────────────────────────────

  val words = sc.parallelize(List("apple", "banana", "apple", "cherry", "banana", "apple"))

  // TODO: Understand reduceByKey aggregates values by key (a shuffle operation)
  val wordCounts: RDD[(String, Int)] = words.map(w => (w, 1)).reduceByKey(_ + _)

  println(s"\n✓ Exercise 5: reduceByKey")
  wordCounts.collect().sortBy(_._1).foreach { case (word, count) =>
    println(s"  $word: $count")
  }
  val appleCount = wordCounts.filter(_._1 == "apple").first()._2
  assert(appleCount == 3, "Apple should appear 3 times")
  println("  ✓ CORRECT!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Chaining transformations
  // ─────────────────────────────────────────────────────────────────
  // Chain multiple transformations:
  //   1. Start with numbers 1-10
  //   2. Double each number
  //   3. Keep only those > 10
  //   4. Collect the result
  //
  // HINT: sc.parallelize(1 to 10).map(...).filter(...).collect()
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand how transformations can be chained together
  val result: Array[Int] = sc.parallelize(1 to 10).map(_ * 2).filter(_ > 10).collect()

  println(s"\n✓ Exercise 6: Chaining")
  println(s"  Result: ${result.mkString(", ")}")
  assert(result.toSet == Set(12, 14, 16, 18, 20), "Should be 12,14,16,18,20")
  println("  ✓ CORRECT!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: distinct - remove duplicates
  // ─────────────────────────────────────────────────────────────────
  // Remove duplicates from the list
  //
  // HINT: rdd.distinct()
  // ─────────────────────────────────────────────────────────────────

  val withDupes = sc.parallelize(List(1, 2, 2, 3, 3, 3, 4, 4, 4, 4))

  // TODO: Understand that distinct() causes a shuffle to remove duplicates
  val unique: RDD[Int] = withDupes.distinct()

  println(s"\n✓ Exercise 7: distinct")
  println(s"  Unique: ${unique.collect().sorted.mkString(", ")}")
  assert(unique.count() == 4, "Should have 4 unique values")
  println("  ✓ CORRECT!")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 03 COMPLETE! Next: kata-04-lazy-evaluation")
  println("=" * 60)
}
