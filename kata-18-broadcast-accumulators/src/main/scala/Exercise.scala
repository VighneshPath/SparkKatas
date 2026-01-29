import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
 * KATA 18: Broadcast Variables and Accumulators
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 18: Broadcast Variables and Accumulators")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata18")
    .master("local[4]")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: The problem without broadcast
  // ─────────────────────────────────────────────────────────────────
  // Without broadcast, lookup table sent with every task!
  // ─────────────────────────────────────────────────────────────────

  val countryCodeMap = Map(
    "US" -> "United States",
    "UK" -> "United Kingdom",
    "CA" -> "Canada",
    "DE" -> "Germany",
    "FR" -> "France"
  )

  val orders = sc.parallelize(Seq(
    ("order1", "US", 100.0),
    ("order2", "UK", 200.0),
    ("order3", "CA", 150.0),
    ("order4", "US", 300.0),
    ("order5", "DE", 250.0)
  ), 4)

  println(s"\n✓ Exercise 1: Without broadcast")
  // countryCodeMap is captured in closure and sent to each task
  val withoutBroadcast = orders.map { case (id, code, amount) =>
    (id, countryCodeMap.getOrElse(code, "Unknown"), amount)
  }.collect()
  println(s"  Results: ${withoutBroadcast.mkString(", ")}")
  println(s"  Map was sent with each of ${orders.getNumPartitions} tasks!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Using broadcast
  // ─────────────────────────────────────────────────────────────────
  // Broadcast sends data once per executor, not per task
  //
  // TODO: Create a broadcast variable from countryCodeMap
  // HINT: sc.broadcast(data)
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 2: With broadcast")
  // TODO: Understand that broadcast() sends data once per executor, not per task
  val broadcastMap = sc.broadcast(countryCodeMap)

  val withBroadcast = orders.map { case (id, code, amount) =>
    (id, broadcastMap.value.getOrElse(code, "Unknown"), amount)
  }.collect()
  println(s"  Results: ${withBroadcast.mkString(", ")}")
  println(s"  Map sent once per executor, shared by all tasks!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Accumulator for counting
  // ─────────────────────────────────────────────────────────────────
  // Accumulators aggregate values from workers to driver
  //
  // TODO: Create a LongAccumulator to count errors
  // HINT: sc.longAccumulator("name")
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 3: Accumulator for counting")

  val records = sc.parallelize(Seq(
    ("valid", 100),
    ("error", -1),
    ("valid", 200),
    ("error", -1),
    ("valid", 150),
    ("error", -1),
    ("valid", 300)
  ), 2)

  // TODO: Understand that accumulators are write-only on workers, read on driver
  val errorCount = sc.longAccumulator("errorCount")

  val validRecords = records.filter { case (status, value) =>
    if (status == "error") {
      errorCount.add(1)  // Count error
      false
    } else {
      true
    }
  }.collect()

  println(s"  Valid records: ${validRecords.length}")
  println(s"  Error count: ${errorCount.value}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Accumulator gotcha - recomputation
  // ─────────────────────────────────────────────────────────────────
  // Accumulators in transformations can be updated multiple times!
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 4: Accumulator gotcha")

  val counter = sc.longAccumulator("counter")

  val rdd = sc.parallelize(1 to 10, 2).map { x =>
    counter.add(1)
    x * 2
  }

  rdd.count()  // First action
  println(s"  After first action: ${counter.value}")

  rdd.collect()  // Second action - recomputes RDD!
  println(s"  After second action: ${counter.value} (counted twice!)")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Safe accumulator usage with cache
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 5: Safe accumulator with caching")

  val safeCounter = sc.longAccumulator("safeCounter")

  val cachedRdd = sc.parallelize(1 to 10, 2).map { x =>
    safeCounter.add(1)
    x * 2
  }.cache()

  cachedRdd.count()  // First action - caches result
  println(s"  After first action: ${safeCounter.value}")

  cachedRdd.collect()  // Second action - uses cache, no recomputation
  println(s"  After second action: ${safeCounter.value} (same value!)")

  cachedRdd.unpersist()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Broadcast join pattern
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 6: Broadcast join")

  val products = Map(
    1 -> "Laptop",
    2 -> "Phone",
    3 -> "Tablet"
  )

  val sales = sc.parallelize(Seq(
    (1, 1000.0),
    (2, 500.0),
    (1, 1200.0),
    (3, 600.0),
    (2, 450.0)
  ), 2)

  val broadcastProducts = sc.broadcast(products)

  val salesWithNames = sales.map { case (productId, amount) =>
    val productName = broadcastProducts.value.getOrElse(productId, "Unknown")
    (productName, amount)
  }.reduceByKey(_ + _).collect()

  println(s"  Sales by product: ${salesWithNames.mkString(", ")}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Multiple accumulators
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 7: Multiple accumulators")

  val successCount = sc.longAccumulator("success")
  val failureCount = sc.longAccumulator("failure")
  val totalAmount = sc.doubleAccumulator("totalAmount")

  val transactions = sc.parallelize(Seq(
    ("success", 100.0),
    ("failure", 0.0),
    ("success", 200.0),
    ("success", 150.0),
    ("failure", 0.0)
  ), 2)

  transactions.foreach { case (status, amount) =>
    if (status == "success") {
      successCount.add(1)
      totalAmount.add(amount)
    } else {
      failureCount.add(1)
    }
  }

  println(s"  Successful: ${successCount.value}")
  println(s"  Failed: ${failureCount.value}")
  println(s"  Total amount: ${totalAmount.value}")

  // Cleanup
  broadcastMap.unpersist()
  broadcastProducts.unpersist()

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 18 COMPLETE!")
  println("=" * 60)
}
