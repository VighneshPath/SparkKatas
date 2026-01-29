import org.apache.spark.sql.SparkSession

/**
 * KATA 21: Cluster Deployment
 *
 * Learn about deploying Spark applications to production clusters.
 * This exercise demonstrates concepts that apply to YARN, Kubernetes, and Standalone.
 *
 * Run locally: sbt run
 * Deploy to cluster: spark-submit --master yarn --deploy-mode cluster target/scala-2.12/kata-21*.jar
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 21: Cluster Deployment")
  println("=" * 60)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Create SparkSession (cluster-ready)
  // ─────────────────────────────────────────────────────────────────
  // Notice: No hardcoded master - let spark-submit provide it
  // For local testing, we use local[*]
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand how this config adapts to local vs cluster
  val spark = SparkSession.builder()
    .appName("Kata21-ClusterDeployment")
    .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))  // Flexible master
    .config("spark.sql.adaptive.enabled", "true")  // AQE for production
    .getOrCreate()

  import spark.implicits._
  val sc = spark.sparkContext

  println(s"\n✓ Exercise 1: SparkSession created")
  println(s"  App Name: ${spark.conf.get("spark.app.name")}")
  println(s"  Master: ${sc.master}")
  println(s"  Executors: ${sc.getExecutorMemoryStatus.size}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Check deployment configuration
  // ─────────────────────────────────────────────────────────────────
  // Understand what configuration is set
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 2: Configuration check")

  // TODO: Examine these key production settings
  val configs = Map(
    "spark.sql.adaptive.enabled" -> spark.conf.getOption("spark.sql.adaptive.enabled"),
    "spark.serializer" -> spark.conf.getOption("spark.serializer"),
    "spark.sql.shuffle.partitions" -> spark.conf.getOption("spark.sql.shuffle.partitions"),
    "spark.eventLog.enabled" -> spark.conf.getOption("spark.eventLog.enabled")
  )

  configs.foreach { case (key, value) =>
    println(s"  $key = ${value.getOrElse("(not set)")}")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Simulate a production workload
  // ─────────────────────────────────────────────────────────────────
  // Create data, perform shuffle, measure performance
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 3: Production workload simulation")

  // Create sample data
  val numRecords = 100000
  val data = (1 to numRecords).map(i => (i % 100, s"user_$i", i * 1.5))
  val df = data.toDF("category", "user", "value")

  // TODO: This is a typical ETL pattern - understand each step
  val startTime = System.currentTimeMillis()

  val result = df
    .groupBy("category")
    .agg(
      org.apache.spark.sql.functions.count("*").as("count"),
      org.apache.spark.sql.functions.sum("value").as("total"),
      org.apache.spark.sql.functions.avg("value").as("average")
    )
    .orderBy("category")

  val count = result.count()
  val endTime = System.currentTimeMillis()

  println(s"  Processed $numRecords records")
  println(s"  Result categories: $count")
  println(s"  Duration: ${endTime - startTime}ms")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Understand the execution plan
  // ─────────────────────────────────────────────────────────────────
  // The explain output shows how Spark will execute the query
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 4: Execution plan analysis")

  // TODO: Look for these patterns in production:
  // - Exchange (shuffle)
  // - HashAggregate (aggregation)
  // - Sort (ordering)
  println("  Physical Plan:")
  result.explain(mode = "simple")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Deployment checklist review
  // ─────────────────────────────────────────────────────────────────
  // Before deploying to production, verify these items
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 5: Deployment checklist")

  val checklist = List(
    ("No hardcoded master", !sc.master.contains("spark://")),
    ("AQE enabled", spark.conf.get("spark.sql.adaptive.enabled", "false") == "true"),
    ("App name set", spark.conf.get("spark.app.name").nonEmpty),
    ("Running locally (for this test)", sc.master.startsWith("local"))
  )

  checklist.foreach { case (item, passed) =>
    val status = if (passed) "✓" else "✗"
    println(s"  $status $item")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: spark-submit command generator
  // ─────────────────────────────────────────────────────────────────
  // Generate example spark-submit commands for different clusters
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 6: Example spark-submit commands")

  val jarPath = "target/scala-2.12/kata-21-cluster-deployment_2.12-1.0.jar"

  println("\n  YARN cluster mode:")
  println(s"""  spark-submit \\
    |    --class Exercise \\
    |    --master yarn \\
    |    --deploy-mode cluster \\
    |    --executor-memory 4g \\
    |    --num-executors 10 \\
    |    --executor-cores 2 \\
    |    --conf spark.sql.adaptive.enabled=true \\
    |    --conf spark.eventLog.enabled=true \\
    |    $jarPath
    |""".stripMargin)

  println("  Kubernetes:")
  println(s"""  spark-submit \\
    |    --class Exercise \\
    |    --master k8s://https://<k8s-api>:6443 \\
    |    --deploy-mode cluster \\
    |    --conf spark.kubernetes.container.image=spark:3.5.0 \\
    |    --conf spark.executor.instances=5 \\
    |    local:///opt/spark/work-dir/kata-21.jar
    |""".stripMargin)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Standalone cluster commands
  // ─────────────────────────────────────────────────────────────────
  // Commands to set up and use a Spark Standalone cluster
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 7: Standalone cluster commands")

  println("""
  # Start Spark Standalone Master:
  $SPARK_HOME/sbin/start-master.sh
  # Access Master UI: http://localhost:8080

  # Start Worker(s):
  $SPARK_HOME/sbin/start-worker.sh spark://master-host:7077

  # Submit to Standalone:
  spark-submit \
    --master spark://master-host:7077 \
    --deploy-mode client \
    --executor-memory 4g \
    --total-executor-cores 20 \
    myapp.jar
  """)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 8: Debug information
  // ─────────────────────────────────────────────────────────────────
  // Show information useful for debugging deployed applications
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 8: Debug information")

  println(s"  Spark version: ${spark.version}")
  println(s"  Scala version: ${util.Properties.versionString}")
  println(s"  Java version: ${System.getProperty("java.version")}")
  println(s"  Default parallelism: ${sc.defaultParallelism}")

  val memoryStatus = sc.getExecutorMemoryStatus
  println(s"  Executor count: ${memoryStatus.size}")
  memoryStatus.foreach { case (executor, (maxMem, freeMem)) =>
    println(f"    $executor: ${maxMem / 1024 / 1024}%,dMB max, ${freeMem / 1024 / 1024}%,dMB free")
  }

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 21 COMPLETE! Next: kata-22-history-server-debugging")
  println("=" * 60)
}
