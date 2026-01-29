import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 09: DataFrames
 *
 * Learn DataFrames - Spark's structured data abstraction.
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 09: DataFrames")
  println("=" * 60)

  val spark = SparkSession.builder()
    .appName("Kata09")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Create DataFrame from sequence
  // ─────────────────────────────────────────────────────────────────
  // Create a DataFrame from a sequence of tuples
  //
  // HINT: seq.toDF("col1", "col2")
  // ─────────────────────────────────────────────────────────────────

  val peopleData = Seq(
    ("Alice", 25, "NYC"),
    ("Bob", 30, "LA"),
    ("Charlie", 35, "NYC"),
    ("Diana", 28, "LA"),
    ("Eve", 32, "NYC")
  )

  // TODO: Understand how to create DataFrames from sequences
  val peopleDF = peopleData.toDF("name", "age", "city")

  println(s"\n✓ Exercise 1: Create DataFrame")
  peopleDF.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: View schema
  // ─────────────────────────────────────────────────────────────────
  // DataFrames have schema - column names and types
  //
  // HINT: df.printSchema()
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 2: Schema")
  // TODO: Understand DataFrame schema with column names and types
  peopleDF.printSchema()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Select columns
  // ─────────────────────────────────────────────────────────────────
  // Choose specific columns from DataFrame
  //
  // HINT: df.select("col1", "col2") or df.select($"col1", $"col2")
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand column selection with select()
  val namesAndAges = peopleDF.select("name", "age")

  println(s"\n✓ Exercise 3: Select columns")
  namesAndAges.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Filter rows
  // ─────────────────────────────────────────────────────────────────
  // Filter DataFrame rows based on condition
  //
  // HINT: df.filter($"column" > value) or df.where("column > value")
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand row filtering with filter() or where()
  val over30 = peopleDF.filter($"age" > 30)

  println(s"\n✓ Exercise 4: Filter rows")
  over30.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Add computed column
  // ─────────────────────────────────────────────────────────────────
  // Create a new column based on existing columns
  //
  // HINT: df.withColumn("newCol", $"existingCol" * 2)
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand adding computed columns with withColumn()
  val withAgeInMonths = peopleDF.withColumn("ageInMonths", $"age" * 12)

  println(s"\n✓ Exercise 5: Add computed column")
  withAgeInMonths.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: GroupBy and aggregate
  // ─────────────────────────────────────────────────────────────────
  // Group by city and calculate average age
  //
  // HINT: df.groupBy("col").agg(avg("col2"))
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand groupBy() and aggregate functions
  val avgAgeByCity = peopleDF.groupBy("city").agg(avg("age"))

  println(s"\n✓ Exercise 6: GroupBy and aggregate")
  avgAgeByCity.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 7: Order by column
  // ─────────────────────────────────────────────────────────────────
  // Sort DataFrame by column
  //
  // HINT: df.orderBy($"col".desc) or df.orderBy(desc("col"))
  // ─────────────────────────────────────────────────────────────────

  // TODO: Understand sorting with orderBy()
  val orderedByAge = peopleDF.orderBy($"age".desc)

  println(s"\n✓ Exercise 7: Order by column")
  orderedByAge.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 8: Join DataFrames
  // ─────────────────────────────────────────────────────────────────
  // Join two DataFrames on a common column
  //
  // HINT: df1.join(df2, "commonCol")
  // ─────────────────────────────────────────────────────────────────

  val salaries = Seq(
    ("Alice", 50000),
    ("Bob", 60000),
    ("Charlie", 70000),
    ("Diana", 55000)
  ).toDF("name", "salary")

  // TODO: Understand DataFrame joins
  val peopleWithSalaries = peopleDF.join(salaries, "name")

  println(s"\n✓ Exercise 8: Join DataFrames")
  peopleWithSalaries.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 9: SQL queries
  // ─────────────────────────────────────────────────────────────────
  // Register DataFrame as temp view and query with SQL
  //
  // HINT: df.createOrReplaceTempView("tableName")
  //       spark.sql("SELECT * FROM tableName")
  // ─────────────────────────────────────────────────────────────────

  peopleDF.createOrReplaceTempView("people")

  // TODO: Understand how to use SQL queries on DataFrames
  val sqlResult = spark.sql("SELECT * FROM people WHERE city = 'NYC'")

  println(s"\n✓ Exercise 9: SQL queries")
  sqlResult.show()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 10: DataFrame to RDD conversion
  // ─────────────────────────────────────────────────────────────────
  // Convert DataFrame back to RDD when needed
  //
  // HINT: df.rdd gives RDD[Row]
  // ─────────────────────────────────────────────────────────────────

  val rdd = peopleDF.rdd

  println(s"\n✓ Exercise 10: DataFrame to RDD")
  println(s"  RDD type: ${rdd.getClass.getSimpleName}")
  println(s"  First row: ${rdd.first()}")
  println(s"  First row name: ${rdd.first().getString(0)}")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 09 COMPLETE!")
  println("=" * 60)
}
