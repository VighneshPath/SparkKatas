import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 10: Catalyst Optimizer - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 10: Catalyst Optimizer - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata10").master("local[4]").getOrCreate()
  import spark.implicits._

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

  // EXERCISE 1
  val simpleQuery = orders.filter($"amount" > 150).select("orderId", "amount")
  println(s"\n✓ Exercise 1:")
  simpleQuery.explain()

  // EXERCISE 2
  println(s"\n✓ Exercise 2: Extended Plan")
  simpleQuery.explain(true)

  // EXERCISE 3
  val joinedFiltered = orders
    .join(customers, orders("custId") === customers("id"))
    .filter($"country" === "USA")
  println(s"\n✓ Exercise 3: Predicate Pushdown")
  joinedFiltered.explain()

  // EXERCISE 4
  val prunedQuery = orders.select("orderId").filter($"orderId" > 2)
  println(s"\n✓ Exercise 4: Column Pruning")
  prunedQuery.explain()

  // EXERCISE 5
  val constantQuery = orders.filter($"amount" > (100 + 50))
  println(s"\n✓ Exercise 5: Constant Folding")
  constantQuery.explain()

  // EXERCISE 6
  val complexQuery = orders
    .join(customers, orders("custId") === customers("id"))
    .groupBy("country")
    .agg(sum("amount").as("totalAmount"))
  println(s"\n✓ Exercise 6: Formatted Plan")
  complexQuery.explain("formatted")

  // EXERCISE 7
  println(s"\n✓ Exercise 7: Join Strategies")
  val defaultJoin = orders.join(customers, orders("custId") === customers("id"))
  defaultJoin.explain()
  val broadcastJoin = orders.join(broadcast(customers), orders("custId") === customers("id"))
  broadcastJoin.explain()

  // EXERCISE 8
  println(s"\n✓ Exercise 8:")
  complexQuery.show()

  // EXERCISE 9
  val inefficientLooking = orders.select("*").filter($"amount" > 100).select("orderId", "amount").filter($"amount" > 150)
  val cleanQuery = orders.filter($"amount" > 150).select("orderId", "amount")
  println(s"\n✓ Exercise 9:")
  inefficientLooking.explain()
  cleanQuery.explain()

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 10 SOLUTION COMPLETE!")
  println("=" * 60)
}
