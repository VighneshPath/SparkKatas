import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * KATA 22: History Server & Debugging
 *
 * Learn to debug Spark applications and understand optimization techniques.
 * Run with: sbt run
 * Open http://localhost:4040 while running to see Spark UI
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 22: History Server & Debugging")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata22-Debugging")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    // Event logging (would go to HDFS/S3 in production)
    // .config("spark.eventLog.enabled", "true")
    // .config("spark.eventLog.dir", "file:///tmp/spark-events")
    .getOrCreate()

  import spark.implicits._
  val sc = spark.sparkContext

  println(s"\n✓ SparkSession created")
  println(s"  Spark UI: http://localhost:4040")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Understand Event Logging Configuration
  // ─────────────────────────────────────────────────────────────────
  // Event logging is required for History Server
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 1: Event Logging Configuration")

  // TODO: Understand these settings for History Server
  val eventLogSettings = Map(
    "spark.eventLog.enabled" -> spark.conf.getOption("spark.eventLog.enabled").getOrElse("false"),
    "spark.eventLog.dir" -> spark.conf.getOption("spark.eventLog.dir").getOrElse("(not set)"),
    "spark.eventLog.compress" -> spark.conf.getOption("spark.eventLog.compress").getOrElse("false")
  )

  println("  Event Log Settings (for History Server):")
  eventLogSettings.foreach { case (k, v) => println(s"    $k = $v") }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Create data with potential skew
  // ─────────────────────────────────────────────────────────────────
  // Data skew is a common production issue
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 2: Data Skew Detection")

  // Create skewed data - key "A" has 80% of the data
  val skewedData = (1 to 10000).map { i =>
    val key = if (i <= 8000) "A" else if (i <= 9000) "B" else "C"
    (key, s"value_$i", i * 1.5)
  }
  val skewedDF = skewedData.toDF("key", "name", "value")

  // TODO: Identify the skewed key
  println("  Key distribution (looking for skew):")
  val keyDistribution = skewedDF.groupBy("key").count().orderBy(desc("count"))
  keyDistribution.show()

  // In Spark UI Stages tab, you would see one task taking much longer

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Demonstrate shuffle operation
  // ─────────────────────────────────────────────────────────────────
  // Shuffles are expensive - check Spark UI for shuffle read/write
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 3: Shuffle Analysis")

  val shuffleResult = skewedDF
    .groupBy("key")
    .agg(
      sum("value").as("total"),
      count("*").as("count")
    )

  // TODO: Check Spark UI -> Stages tab for shuffle metrics
  println("  Aggregation result (triggers shuffle):")
  shuffleResult.show()

  println("  Check Spark UI -> Stages tab for:")
  println("    - Shuffle Read/Write sizes")
  println("    - Task duration distribution")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Caching analysis
  // ─────────────────────────────────────────────────────────────────
  // Check Storage tab to see cached data
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 4: Caching Analysis")

  // Cache the DataFrame
  skewedDF.persist(StorageLevel.MEMORY_AND_DISK)

  // Trigger caching with an action
  val cachedCount = skewedDF.count()
  println(s"  Cached $cachedCount rows")

  // TODO: Check Spark UI -> Storage tab
  println("  Check Spark UI -> Storage tab for:")
  println("    - Memory used")
  println("    - Disk used (if spilled)")
  println("    - Fraction cached")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Execution plan analysis
  // ─────────────────────────────────────────────────────────────────
  // Use explain() to understand query execution
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 5: Execution Plan Analysis")

  // Create a join scenario
  val dim = Seq(("A", "Category A"), ("B", "Category B"), ("C", "Category C"))
    .toDF("key", "category")

  val joined = skewedDF.join(dim, "key")

  // TODO: Look for broadcast join vs shuffle join
  println("  Join execution plan:")
  joined.explain(mode = "simple")

  println("\n  Look for:")
  println("    - BroadcastHashJoin (efficient for small tables)")
  println("    - SortMergeJoin (for large-large joins)")
  println("    - Exchange (indicates shuffle)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: AQE (Adaptive Query Execution) effects
  // ─────────────────────────────────────────────────────────────────
  // AQE optimizes queries at runtime
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 6: Adaptive Query Execution")

  val aqeEnabled = spark.conf.get("spark.sql.adaptive.enabled", "false")
  println(s"  AQE enabled: $aqeEnabled")

  if (aqeEnabled == "true") {
    println("  AQE will automatically:")
    println("    - Coalesce small shuffle partitions")
    println("    - Convert joins to broadcast at runtime")
    println("    - Handle skewed joins")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Memory and GC analysis
  // ─────────────────────────────────────────────────────────────────
  // Check Executors tab for memory and GC metrics
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 7: Memory Analysis")

  val memoryStatus = sc.getExecutorMemoryStatus
  println("  Executor Memory Status:")
  memoryStatus.foreach { case (executor, (maxMem, freeMem)) =>
    val usedMem = maxMem - freeMem
    val usedPercent = (usedMem.toDouble / maxMem * 100).toInt
    println(f"    $executor: ${maxMem / 1024 / 1024}MB max, ${usedPercent}%% used")
  }

  println("\n  Check Spark UI -> Executors tab for:")
  println("    - GC Time (>10% indicates memory pressure)")
  println("    - Peak memory usage")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 8: Common error scenarios (simulated)
  // ─────────────────────────────────────────────────────────────────
  // Understanding common errors helps with debugging
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 8: Common Error Patterns")

  println("  Common errors and their solutions:")
  println()
  println("  1. OutOfMemoryError: Java heap space")
  println("     Solution: Increase --executor-memory or reduce partition size")
  println()
  println("  2. Container killed by YARN for exceeding memory")
  println("     Solution: Increase spark.executor.memoryOverhead")
  println()
  println("  3. Task not serializable")
  println("     Solution: Check closures for non-serializable objects")
  println()
  println("  4. Executor lost")
  println("     Solution: Check executor logs for OOM or other crashes")

  // Unpersist cached data
  skewedDF.unpersist()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 9: History Server commands reference
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 9: History Server Quick Reference")

  println("""
  # Enable event logging in spark-submit:
  spark-submit \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///spark-history \
    myapp.jar

  # Start History Server:
  $SPARK_HOME/sbin/start-history-server.sh

  # Access History Server:
  http://history-server:18080

  # Get YARN logs:
  yarn logs -applicationId application_xxx
  """)

  // Keep app running briefly for UI exploration
  println("\n" + "=" * 60)
  println("Explore http://localhost:4040 now!")
  println("Press Ctrl+C when done exploring")
  println("=" * 60)

  // Brief pause for UI exploration
  Thread.sleep(5000)

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 22 COMPLETE!")
  println("You've completed all 22 Spark Katas!")
  println("=" * 60)
}
