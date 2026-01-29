import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * KATA 22: History Server & Debugging - Solution
 *
 * Comprehensive debugging and optimization demonstration.
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 22: History Server & Debugging - Solution")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata22-Debugging-Solution")
    .master("local[*]")
    // Production-recommended settings
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // Uncomment for History Server:
    // .config("spark.eventLog.enabled", "true")
    // .config("spark.eventLog.dir", "file:///tmp/spark-events")
    .getOrCreate()

  import spark.implicits._
  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // SOLUTION 1: Event Logging Configuration
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Solution 1: Event Logging for History Server")

  println("""
  To enable History Server, add to spark-defaults.conf:

  spark.eventLog.enabled           true
  spark.eventLog.dir               hdfs:///spark-history
  spark.eventLog.compress          true

  Then configure history-server.conf:

  spark.history.fs.logDirectory    hdfs:///spark-history
  spark.history.retainedApplications 50
  spark.history.fs.cleaner.enabled true
  spark.history.fs.cleaner.maxAge  7d

  Start: $SPARK_HOME/sbin/start-history-server.sh
  Access: http://history-server:18080
  """)

  // ─────────────────────────────────────────────────────────────────
  // SOLUTION 2: Data Skew Detection and Handling
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Solution 2: Handling Data Skew")

  // Skewed data
  val skewedData = (1 to 10000).map { i =>
    val key = if (i <= 8000) "A" else if (i <= 9000) "B" else "C"
    (key, s"value_$i", i * 1.5)
  }
  val skewedDF = skewedData.toDF("key", "name", "value")

  // Detect skew
  println("  Detecting skew:")
  skewedDF.groupBy("key").count().orderBy(desc("count")).show()

  // Solution: Salting for skewed keys
  println("  Solution: Salting technique")
  val numSalts = 4
  val saltedDF = skewedDF
    .withColumn("salt", (rand() * numSalts).cast("int"))
    .withColumn("salted_key", concat($"key", lit("_"), $"salt"))

  println("  Salted keys distribution:")
  saltedDF.groupBy("salted_key").count().orderBy(desc("count")).show(10)

  // ─────────────────────────────────────────────────────────────────
  // SOLUTION 3: Optimized Join with Broadcast
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Solution 3: Optimized Joins")

  val dim = Seq(("A", "Category A"), ("B", "Category B"), ("C", "Category C"))
    .toDF("key", "category")

  // Force broadcast for small table
  val optimizedJoin = skewedDF.join(broadcast(dim), "key")

  println("  Broadcast join plan (no shuffle for dim table):")
  optimizedJoin.explain(mode = "simple")

  // ─────────────────────────────────────────────────────────────────
  // SOLUTION 4: Proper Caching Strategy
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Solution 4: Caching Strategy")

  // Only cache if reused
  val reusedDF = skewedDF.filter($"value" > 100)
  reusedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

  // Use it multiple times
  val count1 = reusedDF.count()
  val sum1 = reusedDF.agg(sum("value")).collect()(0).getDouble(0)

  println(s"  First use: count=$count1")
  println(s"  Second use: sum=$sum1 (uses cache)")

  // Always unpersist when done
  reusedDF.unpersist()
  println("  Unpersisted when no longer needed")

  // ─────────────────────────────────────────────────────────────────
  // SOLUTION 5: Debugging Checklist
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Solution 5: Production Debugging Checklist")

  println("""
  When a job fails:

  1. Get logs:
     yarn logs -applicationId <app-id> > logs.txt

  2. Search for errors:
     grep -i "exception\|error\|failed" logs.txt

  3. Check Spark UI/History Server:
     - Jobs tab: Which job failed?
     - Stages tab: Which stage failed?
     - Stages tab: Task duration distribution (skew?)
     - Executors tab: GC time >10%? OOM?

  4. Common fixes:
     - OOM: Increase --executor-memory
     - Container killed: Increase memoryOverhead
     - Slow stage: Check for data skew
     - Task not serializable: Refactor closure

  5. Enable verbose logging:
     spark.sparkContext.setLogLevel("DEBUG")
  """)

  // ─────────────────────────────────────────────────────────────────
  // SOLUTION 6: Recommended Production Configuration
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Solution 6: Production Configuration Template")

  println("""
  spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory 16g \
    --num-executors 20 \
    --executor-cores 4 \
    --driver-memory 4g \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///spark-history \
    --conf spark.speculation=true \
    myapp.jar
  """)

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 22 SOLUTION COMPLETE!")
  println("You're ready for production Spark operations!")
  println("=" * 60)
}
