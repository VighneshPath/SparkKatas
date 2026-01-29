import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * KATA 20: Performance Tuning
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 20: Performance Tuning")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata20-PerformanceTuning")
    .master("local[4]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  import spark.implicits._
  val sc = spark.sparkContext

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Partition sizing
  // ─────────────────────────────────────────────────────────────────
  // Optimal: 100-200MB per partition, 2-4 partitions per core

  println(s"\n✓ Exercise 1: Partition sizing")

  val data = spark.range(1000000)
  println(s"  Default partitions: ${data.rdd.getNumPartitions}")

  val repartitioned = data.repartition(8)  // 4 cores * 2
  println(s"  After repartition(8): ${repartitioned.rdd.getNumPartitions}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Detecting data skew
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 2: Detecting data skew")

  // Create skewed data (most values are "hot_key")
  val skewedData = sc.parallelize(
    (1 to 10000).map(_ => ("hot_key", 1)) ++
    (1 to 100).map(_ => ("normal_a", 1)) ++
    (1 to 100).map(_ => ("normal_b", 1)) ++
    (1 to 100).map(_ => ("normal_c", 1))
  , 4).toDF("key", "value")

  val keyDistribution = skewedData.groupBy("key").count().orderBy($"count".desc)
  println("  Key distribution (skewed!):")
  keyDistribution.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Salting to fix skew
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 3: Salting technique")

  val saltBuckets = 10

  // Add salt to spread hot key
  val saltedData = skewedData.withColumn("salted_key",
    concat($"key", lit("_"), (rand() * saltBuckets).cast("int")))

  // Aggregate with salted key
  val partialAgg = saltedData.groupBy("salted_key").agg(sum("value").as("partial_sum"))

  // Remove salt and final aggregation
  val finalAgg = partialAgg
    .withColumn("original_key", split($"salted_key", "_")(0))
    .groupBy("original_key")
    .agg(sum("partial_sum").as("total"))

  println("  After salting, aggregation is distributed:")
  finalAgg.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: reduceByKey vs groupByKey
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 4: reduceByKey vs groupByKey")

  val words = sc.parallelize(
    Seq.fill(10000)("apple") ++ Seq.fill(5000)("banana") ++ Seq.fill(3000)("cherry")
  , 4)

  val pairs = words.map(w => (w, 1))

  // BAD: groupByKey shuffles all values
  val groupByKeyStart = System.currentTimeMillis()
  val withGroupBy = pairs.groupByKey().mapValues(_.sum).collect()
  val groupByKeyTime = System.currentTimeMillis() - groupByKeyStart

  // GOOD: reduceByKey reduces locally first
  val reduceByKeyStart = System.currentTimeMillis()
  val withReduceBy = pairs.reduceByKey(_ + _).collect()
  val reduceByKeyTime = System.currentTimeMillis() - reduceByKeyStart

  println(s"  groupByKey time: ${groupByKeyTime}ms")
  println(s"  reduceByKey time: ${reduceByKeyTime}ms")
  println(s"  reduceByKey is more efficient!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Broadcast join
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 5: Broadcast join optimization")

  val largeTable = spark.range(100000).withColumn("category", ($"id" % 100).cast("int"))
  val smallTable = spark.range(100).toDF("cat_id").withColumn("cat_name", concat(lit("Category_"), $"cat_id"))

  // Without broadcast hint
  val joinStart1 = System.currentTimeMillis()
  val result1 = largeTable.join(smallTable, largeTable("category") === smallTable("cat_id"))
  result1.count()
  val joinTime1 = System.currentTimeMillis() - joinStart1

  // With broadcast hint
  val joinStart2 = System.currentTimeMillis()
  val result2 = largeTable.join(broadcast(smallTable), largeTable("category") === smallTable("cat_id"))
  result2.count()
  val joinTime2 = System.currentTimeMillis() - joinStart2

  println(s"  Without broadcast: ${joinTime1}ms")
  println(s"  With broadcast: ${joinTime2}ms")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Caching at branch points
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 6: Caching at branch points")

  val baseDF = spark.range(100000)
    .withColumn("value", rand() * 1000)
    .withColumn("category", ($"id" % 10).cast("int"))

  // Without caching - base computed twice
  val noCacheStart = System.currentTimeMillis()
  val branch1 = baseDF.groupBy("category").agg(sum("value"))
  val branch2 = baseDF.groupBy("category").agg(avg("value"))
  branch1.collect()
  branch2.collect()
  val noCacheTime = System.currentTimeMillis() - noCacheStart

  // With caching - base computed once
  val cachedBase = baseDF.cache()
  val cacheStart = System.currentTimeMillis()
  val branch1Cached = cachedBase.groupBy("category").agg(sum("value"))
  val branch2Cached = cachedBase.groupBy("category").agg(avg("value"))
  branch1Cached.collect()
  branch2Cached.collect()
  val cacheTime = System.currentTimeMillis() - cacheStart

  println(s"  Without cache: ${noCacheTime}ms")
  println(s"  With cache: ${cacheTime}ms")

  cachedBase.unpersist()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Filter early
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 7: Filter pushdown")

  val ordersDF = spark.range(100000)
    .withColumn("amount", rand() * 1000)
    .withColumn("status", when(rand() > 0.7, "completed").otherwise("pending"))

  // Let Catalyst push filter down
  val filtered = ordersDF
    .filter($"status" === "completed")
    .groupBy("status")
    .agg(sum("amount"))

  println("  Filter pushed down before aggregation:")
  filtered.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 8: Configuration check
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 8: Key configurations")
  println(s"  spark.sql.shuffle.partitions: ${spark.conf.get("spark.sql.shuffle.partitions", "200")}")
  println(s"  spark.sql.adaptive.enabled: ${spark.conf.get("spark.sql.adaptive.enabled")}")
  println(s"  spark.serializer: ${spark.conf.get("spark.serializer")}")
  println(s"  spark.sql.autoBroadcastJoinThreshold: ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "10MB")}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 9: Coalesce vs Repartition
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 9: Coalesce vs Repartition")

  val manyPartitions = spark.range(10000).repartition(100)
  println(s"  Starting partitions: ${manyPartitions.rdd.getNumPartitions}")

  // Coalesce - no shuffle (just merges)
  val coalesced = manyPartitions.coalesce(10)
  println(s"  After coalesce(10): ${coalesced.rdd.getNumPartitions}")

  // Repartition - full shuffle (can increase or decrease)
  val repartitionedSmall = manyPartitions.repartition(10)
  println(s"  After repartition(10): ${repartitionedSmall.rdd.getNumPartitions}")

  println("  Use coalesce to reduce partitions (no shuffle)")
  println("  Use repartition to increase or evenly distribute")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 10: Summary - Performance checklist
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 10: Performance Checklist Summary")
  println("""
  ┌─────────────────────────────────────────────────────────────┐
  │  PERFORMANCE CHECKLIST                                     │
  ├─────────────────────────────────────────────────────────────┤
  │  □ Use columnar formats (Parquet/ORC)                      │
  │  □ Partition data appropriately (100-200MB per partition)  │
  │  □ Handle data skew (salting, separate hot keys)           │
  │  □ Use reduceByKey over groupByKey                         │
  │  □ Broadcast small tables in joins                         │
  │  □ Cache at branch points                                  │
  │  □ Filter early (predicate pushdown)                       │
  │  □ Use Kryo serialization                                  │
  │  □ Enable AQE (Spark 3.x)                                  │
  │  □ Tune shuffle partitions                                 │
  └─────────────────────────────────────────────────────────────┘
  """)

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 20 COMPLETE!")
  println("=" * 60)
  println("\nCongratulations! You've completed all 20 Spark Katas!")
  println("You now have a deep understanding of Spark internals.")
}
