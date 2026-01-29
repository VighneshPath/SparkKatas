import org.apache.spark.sql.SparkSession

/**
 * KATA 04: Lazy Evaluation
 *
 * Prove that transformations are lazy and understand the execution model.
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 04: Lazy Evaluation")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata04")
    .master("local[4]")
    .getOrCreate()
  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Prove transformations are lazy
  // ─────────────────────────────────────────────────────────────────
  // Create a counter that tracks when map() executes.
  // Observe that the counter doesn't change until an action is called.
  // ─────────────────────────────────────────────────────────────────

  var counter = 0

  val rdd = sc.parallelize(1 to 5).map { x =>
    counter += 1  // This runs on executors, but we can still observe timing
    x * 2
  }

  println(s"\n✓ Exercise 1: Prove laziness")
  println(s"  After creating RDD: counter = $counter")
  println("  (Should be 0 - nothing executed yet!)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Trigger execution with an action
  // ─────────────────────────────────────────────────────────────────
  // Call collect() to trigger execution, then check counter again.
  //
  // HINT: rdd.collect()
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that collect() is an action that triggers computation
  val result = rdd.collect()

  println(s"\n✓ Exercise 2: After action")
  println(s"  Result: ${result.mkString(", ")}")
  // Note: counter may still show 0 here because it runs on executors
  // In local mode it might increment. The key is timing!

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Measure transformation time vs action time
  // ─────────────────────────────────────────────────────────────────
  // Time how long transformations take vs actions.
  // Transformations should be nearly instant (just building the plan).
  // ─────────────────────────────────────────────────────────────────

  val largeRDD = sc.parallelize(1 to 1000000)

  val transformStart = System.currentTimeMillis()

  // TODO: Understand that transformations only build the execution plan
  val transformed = largeRDD.map(_ * 2).filter(_ % 3 == 0).map(_ + 1)

  val transformEnd = System.currentTimeMillis()
  val transformTime = transformEnd - transformStart

  println(s"\n✓ Exercise 3: Timing")
  println(s"  Transformation time: ${transformTime}ms (should be very fast!)")

  val actionStart = System.currentTimeMillis()
  val count = transformed.count()  // This triggers actual work
  val actionEnd = System.currentTimeMillis()
  val actionTime = actionEnd - actionStart

  println(s"  Action time: ${actionTime}ms (actual computation)")
  println(s"  Result count: $count")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Multiple actions = multiple executions
  // ─────────────────────────────────────────────────────────────────
  // Without caching, each action recomputes the entire lineage.
  // Call count() twice and observe both take similar time.
  // ─────────────────────────────────────────────────────────────────

  val data = sc.parallelize(1 to 100000).map(_ * 2).filter(_ % 4 == 0)

  val time1Start = System.currentTimeMillis()
  val count1 = data.count()  // First computation
  val time1 = System.currentTimeMillis() - time1Start

  val time2Start = System.currentTimeMillis()
  val count2 = data.count()  // Recomputes everything!
  val time2 = System.currentTimeMillis() - time2Start

  println(s"\n✓ Exercise 4: Multiple actions")
  println(s"  First count: $count1 (${time1}ms)")
  println(s"  Second count: $count2 (${time2}ms)")
  println("  Both take similar time - no automatic caching!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: View the lineage
  // ─────────────────────────────────────────────────────────────────
  // The lineage shows the "recipe" Spark will follow.
  //
  // HINT: data.toDebugString
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand that toDebugString shows the RDD lineage (DAG)
  val lineage: String = data.toDebugString

  println(s"\n✓ Exercise 5: Lineage (the execution plan)")
  lineage.split("\n").foreach(line => println(s"  $line"))

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 04 COMPLETE! Next: kata-05-partitioning")
  println("=" * 60)
}
