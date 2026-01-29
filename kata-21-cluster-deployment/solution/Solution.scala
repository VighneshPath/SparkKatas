import org.apache.spark.sql.SparkSession

/**
 * KATA 21: Cluster Deployment - Solution
 *
 * This solution demonstrates best practices for production deployments.
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 21: Cluster Deployment - Solution")
  println("=" * 60)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Create SparkSession (cluster-ready)
  // ─────────────────────────────────────────────────────────────────
  // SOLUTION: Use environment variable or spark-submit for master
  // This allows the same code to run locally and on cluster

  val spark = SparkSession.builder()
    .appName("Kata21-ClusterDeployment")
    // For production: Let spark-submit provide master
    // For local testing: Use environment variable or default to local
    .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
    // Production-recommended settings
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  import spark.implicits._
  val sc = spark.sparkContext

  println(s"\n✓ Exercise 1: SparkSession created with production settings")
  println(s"  Master: ${sc.master}")
  println(s"  AQE: ${spark.conf.get("spark.sql.adaptive.enabled")}")
  println(s"  Serializer: ${spark.conf.get("spark.serializer")}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Configuration check
  // ─────────────────────────────────────────────────────────────────
  // SOLUTION: Always verify critical settings

  println(s"\n✓ Exercise 2: Configuration verification")

  val productionSettings = List(
    ("spark.sql.adaptive.enabled", "true", "AQE for runtime optimization"),
    ("spark.serializer", "org.apache.spark.serializer.KryoSerializer", "Faster serialization")
  )

  productionSettings.foreach { case (key, expected, description) =>
    val actual = spark.conf.get(key, "(not set)")
    val status = if (actual == expected) "✓" else "✗"
    println(s"  $status $key = $actual ($description)")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Production workload
  // ─────────────────────────────────────────────────────────────────
  // SOLUTION: Typical ETL with monitoring

  println(s"\n✓ Exercise 3: Production workload with metrics")

  val numRecords = 100000
  val data = (1 to numRecords).map(i => (i % 100, s"user_$i", i * 1.5))
  val df = data.toDF("category", "user", "value")

  // Cache if reused (we won't here, just demonstrating)
  // df.cache()

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

  println(s"  Records: $numRecords")
  println(s"  Categories: $count")
  println(s"  Duration: ${endTime - startTime}ms")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Execution plan
  // ─────────────────────────────────────────────────────────────────
  // SOLUTION: Use extended explain for more details

  println(s"\n✓ Exercise 4: Extended execution plan")
  result.explain(mode = "extended")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Deployment checklist
  // ─────────────────────────────────────────────────────────────────
  // SOLUTION: Comprehensive production checklist

  println(s"\n✓ Exercise 5: Production deployment checklist")

  val fullChecklist = List(
    ("Master not hardcoded", !sc.master.contains("spark://") || sc.master.startsWith("local")),
    ("AQE enabled", spark.conf.get("spark.sql.adaptive.enabled", "false") == "true"),
    ("Kryo serializer", spark.conf.get("spark.serializer", "").contains("Kryo")),
    ("App name descriptive", spark.conf.get("spark.app.name").length > 5),
    ("Shuffle partitions considered", spark.conf.getOption("spark.sql.shuffle.partitions").isDefined)
  )

  fullChecklist.foreach { case (item, passed) =>
    val status = if (passed) "✓" else "⚠"
    println(s"  $status $item")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: spark-submit commands
  // ─────────────────────────────────────────────────────────────────
  // SOLUTION: Production-ready commands

  println(s"\n✓ Exercise 6: Production spark-submit templates")

  println("""
  # YARN (recommended for most production clusters)
  spark-submit \
    --class Exercise \
    --master yarn \
    --deploy-mode cluster \
    --queue production \
    --executor-memory 16g \
    --num-executors 20 \
    --executor-cores 4 \
    --driver-memory 4g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=50 \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///spark-history \
    my-app.jar

  # Kubernetes
  spark-submit \
    --class Exercise \
    --master k8s://https://kubernetes-api:6443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.namespace=spark-apps \
    --conf spark.kubernetes.container.image=my-spark:3.5.0 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=10 \
    --conf spark.kubernetes.executor.request.cores=2 \
    --conf spark.kubernetes.executor.limit.cores=4 \
    --conf spark.kubernetes.driver.request.cores=1 \
    --conf spark.sql.adaptive.enabled=true \
    local:///opt/spark/work-dir/my-app.jar
  """)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: History Server
  // ─────────────────────────────────────────────────────────────────
  // SOLUTION: Show required configuration

  println(s"\n✓ Exercise 7: History Server configuration")

  println("""
  # In spark-defaults.conf (on all submit nodes):
  spark.eventLog.enabled=true
  spark.eventLog.dir=hdfs:///spark-history
  spark.eventLog.compress=true

  # On history server node (spark-history-server.conf):
  spark.history.fs.logDirectory=hdfs:///spark-history
  spark.history.fs.update.interval=10s
  spark.history.retainedApplications=50
  spark.history.ui.port=18080
  spark.history.fs.cleaner.enabled=true
  spark.history.fs.cleaner.maxAge=7d

  # Start history server:
  $SPARK_HOME/sbin/start-history-server.sh

  # Access at: http://history-server:18080
  """)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 8: Debug information
  // ─────────────────────────────────────────────────────────────────
  // SOLUTION: Comprehensive debug output

  println(s"\n✓ Exercise 8: Debug and monitoring info")

  println(s"  Spark: ${spark.version}")
  println(s"  Scala: ${util.Properties.versionString}")
  println(s"  Java: ${System.getProperty("java.version")}")
  println(s"  Parallelism: ${sc.defaultParallelism}")
  println(s"  UI: http://localhost:4040 (while running)")

  println("\n  Key debugging commands:")
  println("    yarn logs -applicationId <app-id>")
  println("    kubectl logs <driver-pod> -n <namespace>")
  println("    spark.sparkContext.getExecutorMemoryStatus")
  println("    df.explain(mode=\"extended\")")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 21 SOLUTION COMPLETE!")
  println("Next: kata-22-history-server-debugging")
  println("=" * 60)
}
