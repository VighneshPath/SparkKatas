import org.apache.spark.sql.SparkSession

/**
 * KATA 14: Jobs, Stages, and Tasks - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 14: Jobs, Stages, and Tasks - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata14").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  val rdd = sc.parallelize(1 to 100, 4)
  println(s"\n✓ Exercise 1: 3 jobs")
  rdd.count()
  rdd.collect()
  rdd.reduce(_ + _)

  val words = sc.parallelize(List("a", "b", "a", "c", "b", "a"), 2)
  val wordCounts = words.map(w => (w, 1)).reduceByKey(_ + _)
  println(s"\n✓ Exercise 2: ${wordCounts.toDebugString}")

  val data = sc.parallelize(1 to 1000, 4)
  val result = data.map(_ * 2).filter(_ % 3 == 0).reduce(_ + _)
  println(s"\n✓ Exercise 3: Result=$result")

  val pairs = sc.parallelize(List(("a", 1), ("b", 2), ("a", 3), ("b", 4)), 2)
  val aggregated = pairs.reduceByKey(_ + _)
  val sorted = aggregated.sortByKey()
  println(s"\n✓ Exercise 4: ${sorted.toDebugString}")
  val numStages = 3 // Stage 0: map, Stage 1: reduceByKey, Stage 2: sortByKey
  println(s"  Number of stages: $numStages")

  println(s"\n✓ Exercise 5: Spark UI at http://localhost:4040")

  val complexRdd = sc.parallelize(1 to 10000, 8)
    .map(x => (x % 10, x))
    .reduceByKey(_ + _)
    .map { case (k, v) => (k, v * 2) }
    .sortByKey()
  println(s"\n✓ Exercise 6: ${complexRdd.collect().mkString(", ")}")
  println(s"  Lineage: ${complexRdd.toDebugString}")

  Thread.sleep(5000)
  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 14 SOLUTION COMPLETE!")
  println("=" * 60)
}
