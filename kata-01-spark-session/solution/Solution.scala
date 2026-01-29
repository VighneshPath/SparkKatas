import org.apache.spark.sql.SparkSession

/**
 * KATA 01: SparkSession - SOLUTION
 *
 * Run with: sbt "runMain Solution"
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 01: SparkSession - SOLUTION")
  println("=" * 60)

  // EXERCISE 1: Create a SparkSession
  val spark: SparkSession = SparkSession.builder()
    .appName("Kata01")
    .master("local[*]")
    .getOrCreate()

  println(s"\n✓ Exercise 1: SparkSession created")
  println(s"  Version: ${spark.version}")

  // EXERCISE 2: Get the SparkContext
  val sc = spark.sparkContext

  println(s"\n✓ Exercise 2: SparkContext accessed")
  println(s"  App ID: ${sc.applicationId}")
  println(s"  Master: ${sc.master}")

  // EXERCISE 3: Create a simple RDD and compute sum
  val sum: Int = sc.parallelize(1 to 10).reduce(_ + _)

  println(s"\n✓ Exercise 3: RDD operation completed")
  println(s"  Sum of 1-10: $sum")
  println("  ✓ CORRECT!")

  // EXERCISE 4: Get configuration value
  val appName: String = spark.conf.get("spark.app.name")

  println(s"\n✓ Exercise 4: Configuration accessed")
  println(s"  App Name: $appName")

  // CLEANUP
  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 01 SOLUTION COMPLETE!")
  println("=" * 60)
}
