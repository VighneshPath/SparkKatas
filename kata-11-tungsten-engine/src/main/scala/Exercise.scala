import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * KATA 11: Tungsten Engine
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 11: Tungsten Engine")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata11").master("local[4]").getOrCreate()
  import spark.implicits._

  // EXERCISE 1: Observe Whole-Stage Code Generation
  // Look for "WholeStageCodegen" in the execution plan
  val df = spark.range(1000000).filter($"id" % 2 === 0).selectExpr("id", "id * 2 as doubled")
  println(s"\n✓ Exercise 1: Look for 'WholeStageCodegen':")
  df.explain()

  // EXERCISE 2: Compare RDD vs DataFrame performance
  val sc = spark.sparkContext
  val rddResult = sc.parallelize(1 to 100000).filter(_ % 2 == 0).map(_ * 2).reduce(_ + _)
  val dfResult = spark.range(1, 100001).filter($"id" % 2 === 0).selectExpr("id * 2 as doubled").agg(sum("doubled")).collect()(0).getLong(0)
  println(s"\n✓ Exercise 2: RDD=$rddResult, DataFrame=$dfResult")

  // EXERCISE 3: Examine codegen details
  val simpleDF = spark.range(100).filter($"id" > 50)
  println(s"\n✓ Exercise 3: CodeGen Details")
  // TODO: Understand that codegen generates optimized Java bytecode
  simpleDF.explain("codegen")

  // EXERCISE 4: UnsafeRow - Tungsten's binary format
  val peopleDF = Seq(("Alice", 25), ("Bob", 30)).toDF("name", "age")
  val internalRDD = peopleDF.queryExecution.toRdd
  println(s"\n✓ Exercise 4: Internal row type: ${internalRDD.first().getClass.getName}")

  // EXERCISE 5: Disable codegen to see difference
  spark.conf.set("spark.sql.codegen.wholeStage", "false")
  println(s"\n✓ Exercise 5: Without codegen:")
  spark.range(100).filter($"id" > 50).explain()
  spark.conf.set("spark.sql.codegen.wholeStage", "true")

  // EXERCISE 6: Aggregation with Tungsten (HashAggregate)
  val salesDF = spark.range(1000).selectExpr("id % 10 as category", "rand() * 100 as amount")
  println(s"\n✓ Exercise 6: HashAggregate plan:")
  salesDF.groupBy("category").agg(sum("amount")).explain()

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 11 COMPLETE!")
  println("=" * 60)
}
