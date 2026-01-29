import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 19: Spark SQL Internals
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 19: Spark SQL Internals")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata19")
    .master("local[4]")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()

  import spark.implicits._

  // Create sample data
  val orders = Seq(
    (1, 101, 100.0, "completed"),
    (2, 102, 200.0, "pending"),
    (3, 101, 150.0, "completed"),
    (4, 103, 300.0, "completed"),
    (5, 102, 250.0, "cancelled")
  ).toDF("orderId", "customerId", "amount", "status")

  val customers = Seq(
    (101, "Alice", "USA"),
    (102, "Bob", "UK"),
    (103, "Charlie", "USA")
  ).toDF("id", "name", "country")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Basic explain() - Physical Plan only
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 1: Basic explain()")
  val simpleQuery = orders.filter($"status" === "completed").select("orderId", "amount")
  simpleQuery.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Extended explain(true) - All plans
  // ─────────────────────────────────────────────────────────────────
  // Shows: Parsed → Analyzed → Optimized → Physical

  println(s"\n✓ Exercise 2: Extended explain(true)")
  simpleQuery.explain(true)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Predicate pushdown
  // ─────────────────────────────────────────────────────────────────
  // Filter is pushed down to scan level

  println(s"\n✓ Exercise 3: Predicate pushdown")
  val joinedThenFiltered = orders
    .join(customers, orders("customerId") === customers("id"))
    .filter($"country" === "USA")

  println("  Notice: Filter pushed below join in physical plan")
  joinedThenFiltered.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Column pruning
  // ─────────────────────────────────────────────────────────────────
  // Spark only reads columns that are needed

  println(s"\n✓ Exercise 4: Column pruning")
  val selectFew = orders.select("orderId", "amount")
  println("  Only reads orderId and amount columns:")
  selectFew.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Constant folding
  // ─────────────────────────────────────────────────────────────────
  // Spark evaluates constants at compile time

  println(s"\n✓ Exercise 5: Constant folding")
  val withConstant = orders.filter($"amount" > lit(100) + lit(50))  // 100 + 50 = 150
  println("  Notice: 100 + 50 becomes 150 in plan")
  withConstant.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Join strategy
  // ─────────────────────────────────────────────────────────────────
  // Spark chooses join strategy based on table sizes

  println(s"\n✓ Exercise 6: Join strategy")

  // Default join
  println("  Default join:")
  val defaultJoin = orders.join(customers, orders("customerId") === customers("id"))
  defaultJoin.explain()

  // Broadcast join (force small table to broadcast)
  println("\n  Broadcast join:")
  val broadcastJoin = orders.join(broadcast(customers), orders("customerId") === customers("id"))
  broadcastJoin.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Aggregation plan
  // ─────────────────────────────────────────────────────────────────
  // Look for HashAggregate stages

  println(s"\n✓ Exercise 7: Aggregation plan")
  val aggregated = orders
    .filter($"status" === "completed")
    .groupBy("customerId")
    .agg(sum("amount").as("totalAmount"), count("*").as("orderCount"))

  println("  Notice: Two HashAggregate stages (partial + final)")
  aggregated.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 8: Formatted explain
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 8: Formatted explain")
  aggregated.explain("formatted")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 9: SQL vs DataFrame - same plan!
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 9: SQL vs DataFrame comparison")

  orders.createOrReplaceTempView("orders")
  customers.createOrReplaceTempView("customers")

  val dfResult = orders
    .filter($"status" === "completed")
    .groupBy("customerId")
    .agg(sum("amount"))

  val sqlResult = spark.sql("""
    SELECT customerId, SUM(amount)
    FROM orders
    WHERE status = 'completed'
    GROUP BY customerId
  """)

  println("  DataFrame plan:")
  dfResult.explain()

  println("  SQL plan:")
  sqlResult.explain()

  println("  Both produce the same physical plan!")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 10: Accessing plans programmatically
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 10: Programmatic plan access")

  val queryExec = dfResult.queryExecution

  println(s"  Logical Plan type: ${queryExec.logical.getClass.getSimpleName}")
  println(s"  Optimized Plan type: ${queryExec.optimizedPlan.getClass.getSimpleName}")
  println(s"  Physical Plan type: ${queryExec.sparkPlan.getClass.getSimpleName}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 11: Adaptive Query Execution (AQE)
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 11: AQE Configuration")
  println(s"  spark.sql.adaptive.enabled: ${spark.conf.get("spark.sql.adaptive.enabled")}")
  println(s"  spark.sql.adaptive.coalescePartitions.enabled: ${spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled", "true")}")
  println(s"  spark.sql.adaptive.skewJoin.enabled: ${spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "true")}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 12: Execute and show results
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 12: Execute the optimized query")
  aggregated.show()

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 19 COMPLETE!")
  println("=" * 60)
}
