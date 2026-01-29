import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 12: Joins and Broadcast
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 12: Joins and Broadcast")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata12").master("local[4]").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  // Sample data
  val orders = Seq((1, 101, 100.0), (2, 102, 200.0), (3, 101, 150.0), (4, 103, 300.0)).toDF("orderId", "custId", "amount")
  val customers = Seq((101, "Alice"), (102, "Bob"), (103, "Charlie")).toDF("id", "name")

  // EXERCISE 1: Inner Join
  println(s"\n✓ Exercise 1: Inner Join")
  // TODO: Understand that inner join returns only matching rows from both sides
  val innerJoin = orders.join(customers, orders("custId") === customers("id"))
  innerJoin.show()

  // EXERCISE 2: Left Outer Join
  val allOrders = Seq((1, 101, 100.0), (2, 999, 200.0)).toDF("orderId", "custId", "amount")
  println(s"\n✓ Exercise 2: Left Outer Join")
  // TODO: Understand that left join keeps all rows from left, null for non-matching right
  val leftJoin = allOrders.join(customers, allOrders("custId") === customers("id"), "left")
  leftJoin.show()

  // EXERCISE 3: Broadcast Join (small table)
  println(s"\n✓ Exercise 3: Broadcast Join")
  // TODO: Understand that broadcast() sends small table to all executors to avoid shuffle
  val broadcastJoin = orders.join(broadcast(customers), orders("custId") === customers("id"))
  broadcastJoin.explain()

  // EXERCISE 4: RDD Join
  val ordersRDD = sc.parallelize(List((101, "order1"), (102, "order2"), (101, "order3")))
  val customersRDD = sc.parallelize(List((101, "Alice"), (102, "Bob")))
  println(s"\n✓ Exercise 4: RDD Join")
  // TODO: Understand RDD join on key-value pairs
  val rddJoin = ordersRDD.join(customersRDD)
  rddJoin.collect().foreach(println)

  // EXERCISE 5: Co-grouped RDD
  println(s"\n✓ Exercise 5: CoGroup")
  // TODO: Understand cogroup groups values from both RDDs by key
  val coGrouped = ordersRDD.cogroup(customersRDD)
  coGrouped.collect().foreach(println)

  // EXERCISE 6: Self Join
  val employees = Seq((1, "Alice", None), (2, "Bob", Some(1)), (3, "Charlie", Some(1))).toDF("id", "name", "managerId")
  println(s"\n✓ Exercise 6: Self Join")
  // TODO: Understand self-join pattern for hierarchical data
  val managers = employees.as("e")
    .join(employees.as("m"), $"e.managerId" === $"m.id", "left")
    .select($"e.name".as("employee"), $"m.name".as("manager"))
  managers.show()

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 12 COMPLETE!")
  println("=" * 60)
}
