import org.apache.spark.sql.SparkSession

/**
 * KATA 16: Task Scheduler - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 16: Task Scheduler - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata16-TaskScheduler")
    .master("local[4]")
    .config("spark.ui.enabled", "true")
    .getOrCreate()

  val sc = spark.sparkContext

  // EXERCISE 1
  val rdd = sc.parallelize(1 to 1000, 12)
  println(s"\n✓ Exercise 1: Task waves")
  println(s"  Partitions: ${rdd.getNumPartitions}")
  println(s"  With 4 cores, expect ${rdd.getNumPartitions / 4} waves")
  val result = rdd.map { x => Thread.sleep(100); x * 2 }.reduce(_ + _)
  println(s"  Result: $result")

  // EXERCISE 2
  val cachedRDD = sc.parallelize(1 to 100, 4).cache()
  cachedRDD.count()
  println(s"\n✓ Exercise 2: Locality")
  val sum = cachedRDD.reduce(_ + _)
  println(s"  Sum: $sum")
  cachedRDD.unpersist()

  // EXERCISE 3
  val skewedRDD = sc.parallelize(1 to 100, 4).mapPartitionsWithIndex { (idx, iter) =>
    if (idx == 0) Thread.sleep(2000)
    iter.map(_ * 2)
  }
  println(s"\n✓ Exercise 3: Straggler task")
  val start = System.currentTimeMillis()
  skewedRDD.collect()
  println(s"  Time: ${System.currentTimeMillis() - start}ms")

  // EXERCISE 4
  println(s"\n✓ Exercise 4: Scheduler config")
  println(s"  spark.scheduler.mode: ${spark.conf.get("spark.scheduler.mode", "FIFO")}")
  println(s"  spark.locality.wait: ${spark.conf.get("spark.locality.wait", "3s")}")

  // EXERCISE 5
  println(s"\n✓ Exercise 5: Multiple jobs (FIFO)")
  sc.parallelize(1 to 100).map(_ * 2).count()
  sc.parallelize(1 to 100).filter(_ % 2 == 0).count()
  sc.parallelize(1 to 100).reduce(_ + _)

  Thread.sleep(5000)
  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 16 SOLUTION COMPLETE!")
  println("=" * 60)
}
