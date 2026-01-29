import org.apache.spark.sql.SparkSession

/**
 * KATA 15: DAG Scheduler - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 15: DAG Scheduler - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata15").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  val rdd1 = sc.parallelize(1 to 100, 4)
  val rdd2 = rdd1.map(_ * 2)
  val rdd3 = rdd2.filter(_ % 3 == 0)
  println(s"\n✓ Exercise 1: ${rdd3.toDebugString}")

  val pairs = sc.parallelize(List("a", "b", "a", "c", "b", "a"), 2)
  val counts = pairs.map(w => (w, 1)).reduceByKey(_ + _)
  println(s"\n✓ Exercise 2: ${counts.toDebugString}")

  val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5), 2)
  val lettersRDD = sc.parallelize(List("a", "b", "c", "d", "e"), 2)
  val zipped = numbersRDD.zip(lettersRDD)
  println(s"\n✓ Exercise 3: ${zipped.toDebugString}")

  val rddA = sc.parallelize(1 to 5, 2)
  val rddB = sc.parallelize(6 to 10, 2)
  val unioned = rddA.union(rddB)
  println(s"\n✓ Exercise 4: ${unioned.toDebugString}, partitions=${unioned.getNumPartitions}")

  val base = sc.parallelize(1 to 100, 4)
  val branch1 = base.map(_ * 2)
  val branch2 = base.map(_ * 3)
  val rejoined = branch1.zip(branch2).map { case (a, b) => a + b }
  println(s"\n✓ Exercise 5: ${rejoined.toDebugString}")

  val data = sc.parallelize(1 to 1000, 4)
  val mapped = data.map(x => (x % 10, x))
  val reduced = mapped.reduceByKey(_ + _)
  val sorted = reduced.sortByKey()
  println(s"\n✓ Exercise 6: ${sorted.toDebugString}")
  println(s"  Result: ${sorted.collect().mkString(", ")}")

  val expensiveRDD = sc.parallelize(1 to 100, 4).map { x => Thread.sleep(1); x * x }
  expensiveRDD.cache()
  println(s"\n✓ Exercise 7: sum=${expensiveRDD.reduce(_ + _)}, max=${expensiveRDD.max()}")
  expensiveRDD.unpersist()

  Thread.sleep(3000)
  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 15 SOLUTION COMPLETE!")
  println("=" * 60)
}
