import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 20: Performance Tuning - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 20: Performance Tuning - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata20").master("local[4]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  // Partition sizing
  println(s"\n✓ Exercise 1: Partition sizing")
  val data = spark.range(1000000)
  println(s"  Default: ${data.rdd.getNumPartitions}, After repartition(8): ${data.repartition(8).rdd.getNumPartitions}")

  // Data skew detection
  println(s"\n✓ Exercise 2: Data skew")
  val skewed = sc.parallelize((1 to 10000).map(_ => ("hot", 1)) ++ (1 to 100).map(_ => ("normal", 1)), 4).toDF("key", "value")
  skewed.groupBy("key").count().show()

  // Salting
  println(s"\n✓ Exercise 3: Salting")
  val salted = skewed.withColumn("salted", concat($"key", lit("_"), (rand() * 10).cast("int")))
  val partial = salted.groupBy("salted").agg(sum("value").as("sum"))
  partial.withColumn("key", split($"salted", "_")(0)).groupBy("key").agg(sum("sum")).show()

  // reduceByKey vs groupByKey
  println(s"\n✓ Exercise 4: reduceByKey vs groupByKey")
  val words = sc.parallelize(Seq.fill(10000)("a") ++ Seq.fill(5000)("b"), 4)
  val pairs = words.map(w => (w, 1))
  val t1 = System.currentTimeMillis()
  pairs.groupByKey().mapValues(_.sum).collect()
  println(s"  groupByKey: ${System.currentTimeMillis() - t1}ms")
  val t2 = System.currentTimeMillis()
  pairs.reduceByKey(_ + _).collect()
  println(s"  reduceByKey: ${System.currentTimeMillis() - t2}ms")

  // Broadcast join
  println(s"\n✓ Exercise 5: Broadcast join")
  val large = spark.range(100000).withColumn("cat", ($"id" % 100).cast("int"))
  val small = spark.range(100).toDF("cid").withColumn("name", concat(lit("C"), $"cid"))
  large.join(broadcast(small), large("cat") === small("cid")).count()

  // Caching
  println(s"\n✓ Exercise 6: Caching at branch")
  val base = spark.range(100000).withColumn("v", rand()).cache()
  base.groupBy(($"id" % 10).as("g")).agg(sum("v")).collect()
  base.groupBy(($"id" % 10).as("g")).agg(avg("v")).collect()
  base.unpersist()

  // Filter early
  println(s"\n✓ Exercise 7: Filter pushdown")
  spark.range(100000).withColumn("s", when(rand() > 0.7, "done").otherwise("pending"))
    .filter($"s" === "done").groupBy("s").count().explain()

  // Config
  println(s"\n✓ Exercise 8: Config")
  println(s"  shuffle.partitions: ${spark.conf.get("spark.sql.shuffle.partitions", "200")}")
  println(s"  adaptive.enabled: ${spark.conf.get("spark.sql.adaptive.enabled")}")
  println(s"  serializer: ${spark.conf.get("spark.serializer")}")

  // Coalesce vs Repartition
  println(s"\n✓ Exercise 9: Coalesce vs Repartition")
  val many = spark.range(10000).repartition(100)
  println(s"  coalesce(10): ${many.coalesce(10).rdd.getNumPartitions}")
  println(s"  repartition(10): ${many.repartition(10).rdd.getNumPartitions}")

  println(s"\n✓ Exercise 10: Performance Checklist - See README.md")

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 20 SOLUTION COMPLETE!")
  println("=" * 60)
  println("\n🎉 Congratulations! All 20 Spark Katas completed!")
}
