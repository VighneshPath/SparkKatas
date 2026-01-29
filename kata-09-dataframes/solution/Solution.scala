import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 09: DataFrames - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 09: DataFrames - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata09").master("local[4]").getOrCreate()
  import spark.implicits._

  // EXERCISE 1
  val peopleData = Seq(
    ("Alice", 25, "NYC"),
    ("Bob", 30, "LA"),
    ("Charlie", 35, "NYC"),
    ("Diana", 28, "LA"),
    ("Eve", 32, "NYC")
  )
  val peopleDF = peopleData.toDF("name", "age", "city")
  println(s"\n✓ Exercise 1:")
  peopleDF.show()

  // EXERCISE 2
  println(s"\n✓ Exercise 2:")
  peopleDF.printSchema()

  // EXERCISE 3
  val namesAndAges = peopleDF.select("name", "age")
  println(s"\n✓ Exercise 3:")
  namesAndAges.show()

  // EXERCISE 4
  val over30 = peopleDF.filter($"age" > 30)
  println(s"\n✓ Exercise 4:")
  over30.show()

  // EXERCISE 5
  val withAgeInMonths = peopleDF.withColumn("ageInMonths", $"age" * 12)
  println(s"\n✓ Exercise 5:")
  withAgeInMonths.show()

  // EXERCISE 6
  val avgAgeByCity = peopleDF.groupBy("city").agg(avg("age").as("avgAge"))
  println(s"\n✓ Exercise 6:")
  avgAgeByCity.show()

  // EXERCISE 7
  val orderedByAge = peopleDF.orderBy($"age".desc)
  println(s"\n✓ Exercise 7:")
  orderedByAge.show()

  // EXERCISE 8
  val salaries = Seq(
    ("Alice", 50000), ("Bob", 60000), ("Charlie", 70000), ("Diana", 55000)
  ).toDF("name", "salary")
  val peopleWithSalaries = peopleDF.join(salaries, "name")
  println(s"\n✓ Exercise 8:")
  peopleWithSalaries.show()

  // EXERCISE 9
  peopleDF.createOrReplaceTempView("people")
  val sqlResult = spark.sql("SELECT * FROM people WHERE city = 'NYC'")
  println(s"\n✓ Exercise 9:")
  sqlResult.show()

  // EXERCISE 10
  val rdd = peopleDF.rdd
  println(s"\n✓ Exercise 10: RDD first row = ${rdd.first()}")

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 09 SOLUTION COMPLETE!")
  println("=" * 60)
}
