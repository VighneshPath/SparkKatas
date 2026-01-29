import org.apache.spark.sql.SparkSession

/**
 * KATA 04: Lazy Evaluation - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 04: Lazy Evaluation - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata04").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  // EXERCISE 1
  var counter = 0
  val rdd = sc.parallelize(1 to 5).map { x => counter += 1; x * 2 }
  println(s"\n✓ Exercise 1: counter = $counter (should be 0)")

  // EXERCISE 2
  val result = rdd.collect()
  println(s"\n✓ Exercise 2: ${result.mkString(", ")}")

  // EXERCISE 3
  val largeRDD = sc.parallelize(1 to 1000000)
  val transformStart = System.currentTimeMillis()
  val transformed = largeRDD.map(_ * 2).filter(_ > 100).map(_ + 1)
  val transformTime = System.currentTimeMillis() - transformStart
  println(s"\n✓ Exercise 3: Transform time: ${transformTime}ms")

  val actionStart = System.currentTimeMillis()
  val count = transformed.count()
  val actionTime = System.currentTimeMillis() - actionStart
  println(s"  Action time: ${actionTime}ms, count: $count")

  // EXERCISE 4
  val data = sc.parallelize(1 to 100000).map(_ * 2).filter(_ % 4 == 0)
  val t1 = System.currentTimeMillis()
  val c1 = data.count()
  val time1 = System.currentTimeMillis() - t1
  val t2 = System.currentTimeMillis()
  val c2 = data.count()
  val time2 = System.currentTimeMillis() - t2
  println(s"\n✓ Exercise 4: First: $c1 (${time1}ms), Second: $c2 (${time2}ms)")

  // EXERCISE 5
  val lineage: String = data.toDebugString
  println(s"\n✓ Exercise 5: Lineage")
  lineage.split("\n").foreach(line => println(s"  $line"))

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 04 SOLUTION COMPLETE!")
  println("=" * 60)
}
