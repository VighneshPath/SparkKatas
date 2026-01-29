import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 10: Catalyst Optimizer
 *
 * Learn how Spark's Catalyst optimizer transforms queries.
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 10: Catalyst Optimizer")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata10")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  // Sample data
  val orders = Seq(
    (1, 101, 100.0, "2024-01-01"),
    (2, 102, 200.0, "2024-01-02"),
    (3, 101, 150.0, "2024-01-03"),
    (4, 103, 300.0, "2024-01-04"),
    (5, 102, 250.0, "2024-01-05")
  ).toDF("orderId", "custId", "amount", "date")

  val customers = Seq(
    (101, "Alice", "USA"),
    (102, "Bob", "UK"),
    (103, "Charlie", "USA"),
    (104, "Diana", "Canada")
  ).toDF("id", "name", "country")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: View the Logical Plan
  // ─────────────────────────────────────────────────────────────────
  // Use explain() to see how Catalyst transforms your query
  //
  // HINT: df.explain() or df.explain(true) for extended
  // ─────────────────────────────────────────────────────────────────

  val simpleQuery = orders.filter($"amount" > 150).select("orderId", "amount")

  println(s"\n✓ Exercise 1: View Logical Plan")
  println(s"  Query: orders.filter(amount > 150).select(orderId, amount)")
  println(s"  Execution Plan:")
  simpleQuery.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: View Extended Plan (all stages)
  // ─────────────────────────────────────────────────────────────────
  // explain(true) shows: Parsed, Analyzed, Optimized, Physical plans
  //
  // HINT: df.explain(true)
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 2: Extended Plan")
  // TODO: Understand the 4 plan stages: Parsed, Analyzed, Optimized, Physical
  simpleQuery.explain(true)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Observe Predicate Pushdown
  // ─────────────────────────────────────────────────────────────────
  // Catalyst pushes filters as early as possible
  // Notice how the filter is pushed before the join
  // ─────────────────────────────────────────────────────────────────

  val joinedFiltered = orders
    .join(customers, orders("custId") === customers("id"))
    .filter($"country" === "USA")

  println(s"\n✓ Exercise 3: Predicate Pushdown")
  println(s"  Query: orders.join(customers).filter(country = 'USA')")
  println(s"  Notice how filter is pushed to scan customers first:")
  joinedFiltered.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Observe Column Pruning
  // ─────────────────────────────────────────────────────────────────
  // Catalyst only reads columns that are needed
  // ─────────────────────────────────────────────────────────────────

  val prunedQuery = orders.select("orderId").filter($"orderId" > 2)

  println(s"\n✓ Exercise 4: Column Pruning")
  println(s"  Query: orders.select(orderId).filter(orderId > 2)")
  println(s"  Notice: only 'orderId' is read, other columns pruned:")
  prunedQuery.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Constant Folding
  // ─────────────────────────────────────────────────────────────────
  // Catalyst evaluates constant expressions at compile time
  // ─────────────────────────────────────────────────────────────────

  val constantQuery = orders.filter($"amount" > (100 + 50))  // 100 + 50 evaluated at compile time

  println(s"\n✓ Exercise 5: Constant Folding")
  println(s"  Query: filter(amount > (100 + 50))")
  println(s"  Notice: '100 + 50' is folded to '150' in the plan:")
  constantQuery.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Compare Logical vs Physical Plan
  // ─────────────────────────────────────────────────────────────────
  // Use explain("formatted") for readable output
  //
  // HINT: df.explain("formatted")
  // ─────────────────────────────────────────────────────────────────

  val complexQuery = orders
    .join(customers, orders("custId") === customers("id"))
    .groupBy("country")
    .agg(sum("amount").as("totalAmount"))

  println(s"\n✓ Exercise 6: Formatted Plan")
  // TODO: Understand the formatted explain output
  complexQuery.explain("formatted")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Understand Join Strategies
  // ─────────────────────────────────────────────────────────────────
  // Catalyst chooses join strategy based on data size
  // - BroadcastHashJoin: for small tables
  // - SortMergeJoin: for large tables
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 7: Join Strategies")
  println(s"  Default join (Catalyst chooses strategy):")

  val defaultJoin = orders.join(customers, orders("custId") === customers("id"))
  defaultJoin.explain()

  // Force broadcast join
  val broadcastJoin = orders.join(broadcast(customers), orders("custId") === customers("id"))
  println(s"\n  Forced broadcast join:")
  broadcastJoin.explain()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 8: View Query Statistics
  // ─────────────────────────────────────────────────────────────────
  // Catalyst uses statistics for cost-based optimization
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 8: Query Execution")
  println(s"  Result of complex query:")
  complexQuery.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 9: Inefficient vs Optimized Query
  // ─────────────────────────────────────────────────────────────────
  // Write the same query two ways - Catalyst optimizes both!
  // ─────────────────────────────────────────────────────────────────

  // Inefficient looking query (but Catalyst optimizes it!)
  val inefficientLooking = orders
    .select("*")
    .filter($"amount" > 100)
    .select("orderId", "amount")
    .filter($"amount" > 150)

  // Clean query
  val cleanQuery = orders
    .filter($"amount" > 150)
    .select("orderId", "amount")

  println(s"\n✓ Exercise 9: Both queries have same plan!")
  println(s"  'Inefficient' query plan:")
  inefficientLooking.explain()
  println(s"\n  Clean query plan:")
  cleanQuery.explain()
  println(s"  (Catalyst optimizes both to the same execution!)")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 10 COMPLETE!")
  println("=" * 60)
}
