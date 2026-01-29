import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 19: Spark SQL Internals - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 19: Spark SQL Internals - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata19").master("local[4]")
    .config("spark.sql.adaptive.enabled", "true").getOrCreate()
  import spark.implicits._

  val orders = Seq((1, 101, 100.0, "completed"), (2, 102, 200.0, "pending"), (3, 101, 150.0, "completed")).toDF("orderId", "customerId", "amount", "status")
  val customers = Seq((101, "Alice", "USA"), (102, "Bob", "UK")).toDF("id", "name", "country")

  // Basic explain
  println(s"\n✓ Exercise 1: Basic explain()")
  orders.filter($"status" === "completed").select("orderId", "amount").explain()

  // Extended explain
  println(s"\n✓ Exercise 2: Extended explain(true)")
  orders.filter($"status" === "completed").explain(true)

  // Predicate pushdown
  println(s"\n✓ Exercise 3: Predicate pushdown")
  orders.join(customers, orders("customerId") === customers("id")).filter($"country" === "USA").explain()

  // Column pruning
  println(s"\n✓ Exercise 4: Column pruning")
  orders.select("orderId", "amount").explain()

  // Constant folding
  println(s"\n✓ Exercise 5: Constant folding")
  orders.filter($"amount" > lit(100) + lit(50)).explain()

  // Join strategies
  println(s"\n✓ Exercise 6: Join strategies")
  orders.join(customers, orders("customerId") === customers("id")).explain()
  orders.join(broadcast(customers), orders("customerId") === customers("id")).explain()

  // Aggregation
  println(s"\n✓ Exercise 7: Aggregation plan")
  orders.filter($"status" === "completed").groupBy("customerId").agg(sum("amount")).explain()

  // SQL vs DataFrame
  println(s"\n✓ Exercise 9: SQL vs DataFrame")
  orders.createOrReplaceTempView("orders")
  spark.sql("SELECT customerId, SUM(amount) FROM orders WHERE status='completed' GROUP BY customerId").explain()

  // Programmatic access
  println(s"\n✓ Exercise 10: Programmatic access")
  val q = orders.filter($"status" === "completed").groupBy("customerId").agg(sum("amount"))
  println(s"  Logical: ${q.queryExecution.logical.getClass.getSimpleName}")
  println(s"  Physical: ${q.queryExecution.sparkPlan.getClass.getSimpleName}")

  // AQE config
  println(s"\n✓ Exercise 11: AQE enabled = ${spark.conf.get("spark.sql.adaptive.enabled")}")

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 19 SOLUTION COMPLETE!")
  println("=" * 60)
}
