import org.apache.spark.sql.SparkSession

/**
 * KATA 16: Task Scheduler
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 16: Task Scheduler")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata16-TaskScheduler")
    .master("local[4]")  // 4 cores
    .config("spark.ui.enabled", "true")
    .getOrCreate()

  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Observe task distribution
  // ─────────────────────────────────────────────────────────────────
  // Create an RDD with more partitions than cores to see task waves
  //
  // Open Spark UI at http://localhost:4040 while running
  // ─────────────────────────────────────────────────────────────────

  val rdd = sc.parallelize(1 to 1000, 12)  // 12 partitions, 4 cores = 3 waves

  println(s"\n✓ Exercise 1: Task waves")
  println(s"  Partitions: ${rdd.getNumPartitions}")
  println(s"  With 4 cores, expect ${rdd.getNumPartitions / 4} waves")

  val result = rdd.map { x =>
    Thread.sleep(100)  // Simulate work
    x * 2
  }.reduce(_ + _)

  println(s"  Result: $result")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Understand locality levels
  // ─────────────────────────────────────────────────────────────────
  // Check Spark UI → Stages → click on stage → Summary Metrics
  // Look for "Locality Level" column
  // ─────────────────────────────────────────────────────────────────

  val cachedRDD = sc.parallelize(1 to 100, 4).cache()
  cachedRDD.count()  // Materialize cache

  // Second action should show PROCESS_LOCAL locality
  println(s"\n✓ Exercise 2: Locality")
  println(s"  First action caches data...")
  println(s"  Second action should use PROCESS_LOCAL")
  val sum = cachedRDD.reduce(_ + _)
  println(s"  Sum: $sum")

  cachedRDD.unpersist()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Create a straggler task
  // ─────────────────────────────────────────────────────────────────
  // One partition takes much longer than others
  // ─────────────────────────────────────────────────────────────────

  val skewedRDD = sc.parallelize(1 to 100, 4).mapPartitionsWithIndex { (idx, iter) =>
    if (idx == 0) {
      Thread.sleep(2000)  // Partition 0 is slow (straggler)
    }
    iter.map(_ * 2)
  }

  println(s"\n✓ Exercise 3: Straggler task")
  println(s"  Partition 0 will take 2 seconds longer...")
  val start = System.currentTimeMillis()
  val stragglerResult = skewedRDD.collect()
  println(s"  Time: ${System.currentTimeMillis() - start}ms")
  println(s"  Job waited for slowest task!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Check scheduler configuration
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 4: Scheduler config")
  println(s"  spark.scheduler.mode: ${spark.conf.get("spark.scheduler.mode", "FIFO")}")
  println(s"  spark.locality.wait: ${spark.conf.get("spark.locality.wait", "3s")}")
  println(s"  spark.task.maxFailures: ${spark.conf.get("spark.task.maxFailures", "4")}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Multiple jobs (FIFO scheduling)
  // ─────────────────────────────────────────────────────────────────
  // In FIFO mode, jobs run sequentially
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 5: Multiple jobs (FIFO)")
  println(s"  Running 3 jobs sequentially...")

  val job1Start = System.currentTimeMillis()
  sc.parallelize(1 to 100).map(_ * 2).count()
  println(s"  Job 1 complete: ${System.currentTimeMillis() - job1Start}ms")

  val job2Start = System.currentTimeMillis()
  sc.parallelize(1 to 100).filter(_ % 2 == 0).count()
  println(s"  Job 2 complete: ${System.currentTimeMillis() - job2Start}ms")

  val job3Start = System.currentTimeMillis()
  sc.parallelize(1 to 100).reduce(_ + _)
  println(s"  Job 3 complete: ${System.currentTimeMillis() - job3Start}ms")

  // Keep UI available for inspection
  println("\n  Spark UI available at http://localhost:4040")
  println("  Press Ctrl+C to exit or wait 10 seconds...")
  Thread.sleep(10000)

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 16 COMPLETE!")
  println("=" * 60)
}
