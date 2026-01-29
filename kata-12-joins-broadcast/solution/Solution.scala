import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 12: Joins and Broadcast - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 12: Joins and Broadcast - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata12").master("local[4]").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  val orders = Seq((1, 101, 100.0), (2, 102, 200.0), (3, 101, 150.0), (4, 103, 300.0)).toDF("orderId", "custId", "amount")
  val customers = Seq((101, "Alice"), (102, "Bob"), (103, "Charlie")).toDF("id", "name")

  // EXERCISE 1
  println(s"\n✓ Exercise 1: Inner Join")
  val innerJoin = orders.join(customers, orders("custId") === customers("id"))
  innerJoin.show()

  // EXERCISE 2
  val allOrders = Seq((1, 101, 100.0), (2, 999, 200.0)).toDF("orderId", "custId", "amount")
  println(s"\n✓ Exercise 2: Left Outer Join")
  val leftJoin = allOrders.join(customers, allOrders("custId") === customers("id"), "left_outer")
  leftJoin.show()

  // EXERCISE 3
  println(s"\n✓ Exercise 3: Broadcast Join")
  val broadcastJoin = orders.join(broadcast(customers), orders("custId") === customers("id"))
  broadcastJoin.explain()

  // EXERCISE 4
  val ordersRDD = sc.parallelize(List((101, "order1"), (102, "order2"), (101, "order3")))
  val customersRDD = sc.parallelize(List((101, "Alice"), (102, "Bob")))
  println(s"\n✓ Exercise 4: RDD Join")
  val rddJoin = ordersRDD.join(customersRDD)
  rddJoin.collect().foreach(println)

  // EXERCISE 5
  println(s"\n✓ Exercise 5: CoGroup")
  val coGrouped = ordersRDD.cogroup(customersRDD)
  coGrouped.collect().foreach(println)

  // EXERCISE 6
  val employees = Seq((1, "Alice", None), (2, "Bob", Some(1)), (3, "Charlie", Some(1))).toDF("id", "name", "managerId")
  println(s"\n✓ Exercise 6: Self Join")
  val e = employees.as("e")
  val m = employees.as("m")
  val selfJoin = e.join(m, $"e.managerId" === $"m.id", "left_outer").select($"e.name".as("employee"), $"m.name".as("manager"))
  selfJoin.show()

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 12 SOLUTION COMPLETE!")
  println("=" * 60)
}
