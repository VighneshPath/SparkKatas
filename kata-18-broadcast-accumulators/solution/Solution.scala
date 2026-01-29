import org.apache.spark.sql.SparkSession

/**
 * KATA 18: Broadcast Variables and Accumulators - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 18: Broadcast Variables and Accumulators - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata18").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  val countryCodeMap = Map("US" -> "United States", "UK" -> "United Kingdom", "CA" -> "Canada", "DE" -> "Germany", "FR" -> "France")
  val orders = sc.parallelize(Seq(("order1", "US", 100.0), ("order2", "UK", 200.0), ("order3", "CA", 150.0)), 4)

  // EXERCISE 1-2: Broadcast
  println(s"\n✓ Exercise 1-2: Broadcast variable")
  val broadcastMap = sc.broadcast(countryCodeMap)
  val withBroadcast = orders.map { case (id, code, amount) =>
    (id, broadcastMap.value.getOrElse(code, "Unknown"), amount)
  }.collect()
  println(s"  Results: ${withBroadcast.mkString(", ")}")

  // EXERCISE 3: Accumulator
  println(s"\n✓ Exercise 3: Accumulator")
  val records = sc.parallelize(Seq(("valid", 100), ("error", -1), ("valid", 200), ("error", -1)), 2)
  val errorCount = sc.longAccumulator("errorCount")
  records.filter { case (status, _) =>
    if (status == "error") { errorCount.add(1); false } else true
  }.collect()
  println(s"  Error count: ${errorCount.value}")

  // EXERCISE 4-5: Accumulator gotcha and fix
  println(s"\n✓ Exercise 4-5: Accumulator gotcha")
  val counter = sc.longAccumulator("counter")
  val rdd = sc.parallelize(1 to 10, 2).map { x => counter.add(1); x * 2 }
  rdd.count()
  println(s"  After first action: ${counter.value}")
  rdd.collect()
  println(s"  After second action: ${counter.value} (counted twice!)")

  val safeCounter = sc.longAccumulator("safe")
  val cached = sc.parallelize(1 to 10, 2).map { x => safeCounter.add(1); x * 2 }.cache()
  cached.count()
  cached.collect()
  println(s"  With cache: ${safeCounter.value} (same!)")
  cached.unpersist()

  // EXERCISE 6: Broadcast join
  println(s"\n✓ Exercise 6: Broadcast join")
  val products = Map(1 -> "Laptop", 2 -> "Phone", 3 -> "Tablet")
  val sales = sc.parallelize(Seq((1, 1000.0), (2, 500.0), (1, 1200.0)), 2)
  val broadcastProducts = sc.broadcast(products)
  val result = sales.map { case (id, amount) => (broadcastProducts.value.getOrElse(id, "Unknown"), amount) }.reduceByKey(_ + _).collect()
  println(s"  Sales by product: ${result.mkString(", ")}")

  // EXERCISE 7: Multiple accumulators
  println(s"\n✓ Exercise 7: Multiple accumulators")
  val successCount = sc.longAccumulator("success")
  val totalAmount = sc.doubleAccumulator("total")
  sc.parallelize(Seq(("success", 100.0), ("success", 200.0)), 2).foreach { case (status, amount) =>
    if (status == "success") { successCount.add(1); totalAmount.add(amount) }
  }
  println(s"  Success: ${successCount.value}, Total: ${totalAmount.value}")

  broadcastMap.unpersist()
  broadcastProducts.unpersist()
  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 18 SOLUTION COMPLETE!")
  println("=" * 60)
}
