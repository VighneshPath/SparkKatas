import org.apache.spark.sql.SparkSession

/**
 * KATA 01: SparkSession - The Entry Point
 *
 * Complete the TODOs below to learn how to create and use a SparkSession.
 * Run with: sbt run
 *
 * Each exercise is independent - the kata will run even if some are incomplete.
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 01: SparkSession")
  println("=" * 60)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Create a SparkSession
  // ─────────────────────────────────────────────────────────────────
  // Create a SparkSession with:
  //   - appName: "Kata01"
  //   - master: "local[*]"
  //
  // HINT: SparkSession.builder().appName(...).master(...).getOrCreate()
  // ─────────────────────────────────────────────────────────────────

  // TODO: Replace the line below with your SparkSession creation
  val spark: SparkSession = SparkSession.builder()
    .appName("Kata01")     // TODO: Set your app name
    .master("local[*]")    // TODO: Set master to local[*]
    .getOrCreate()

  println(s"\n✓ Exercise 1: SparkSession created")
  println(s"  Version: ${spark.version}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Get the SparkContext
  // ─────────────────────────────────────────────────────────────────
  // Access the SparkContext from the SparkSession
  //
  // HINT: spark.sparkContext
  // ─────────────────────────────────────────────────────────────────

  val sc = spark.sparkContext  // TODO: Verify this is correct

  println(s"\n✓ Exercise 2: SparkContext accessed")
  println(s"  App ID: ${sc.applicationId}")
  println(s"  Master: ${sc.master}")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Create a simple RDD and compute sum
  // ─────────────────────────────────────────────────────────────────
  // Create an RDD with numbers 1-10 and calculate their sum
  //
  // TODO: Complete this exercise
  // HINT: sc.parallelize(1 to 10).reduce(_ + _)
  // ─────────────────────────────────────────────────────────────────

  val sum: Int = sc.parallelize(1 to 10).reduce(_ + _)  // TODO: Replace 0 with correct calculation

  println(s"\n✓ Exercise 3: RDD operation completed")
  println(s"  Sum of 1-10: $sum")

  // Verify your answer
  if (sum == 55) {
    println("  ✓ CORRECT!")
  } else {
    println(s"  ✗ Expected 55, got $sum")
  }

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Get configuration value
  // ─────────────────────────────────────────────────────────────────
  // Get the app name from configuration
  //
  // TODO: Complete this exercise
  // HINT: spark.conf.get("spark.app.name")
  // ─────────────────────────────────────────────────────────────────

  val appName: String = spark.conf.get("spark.app.name")  // TODO: Replace with correct call

  println(s"\n✓ Exercise 4: Configuration accessed")
  println(s"  App Name: $appName")

  // Verify
  if (appName == "Kata01") {
    println("  ✓ CORRECT!")
  } else {
    println(s"  ✗ Expected 'Kata01', got '$appName'")
  }

  // ─────────────────────────────────────────────────────────────────
  // CLEANUP: Always stop SparkSession when done
  // ─────────────────────────────────────────────────────────────────

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 01 COMPLETE! Next: kata-02-rdd-basics")
  println("=" * 60)
}
