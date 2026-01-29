import org.apache.spark.sql.SparkSession

/**
 * KATA 15: DAG Scheduler
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 15: DAG Scheduler")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata15").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  println("""
  DAG SCHEDULER RESPONSIBILITIES:

  1. Builds DAG of stages from RDD lineage
  2. Splits DAG at shuffle boundaries
  3. Submits stages in correct order (parents first)
  4. Handles stage failures and retries
  5. Determines preferred locations for tasks

  RDD Lineage → DAG → Stages → Tasks
  """)

  // EXERCISE 1: Visualize the DAG through lineage
  val rdd1 = sc.parallelize(1 to 100, 4)
  val rdd2 = rdd1.map(_ * 2)
  val rdd3 = rdd2.filter(_ % 3 == 0)
  println(s"\n✓ Exercise 1: Linear DAG (no shuffle)")
  println(s"  Lineage: ${rdd3.toDebugString}")
  println(s"  All operations fuse into 1 stage")

  // EXERCISE 2: DAG with shuffle boundary
  val pairs = sc.parallelize(List("a", "b", "a", "c", "b", "a"), 2)
  val counts = pairs.map(w => (w, 1)).reduceByKey(_ + _)
  println(s"\n✓ Exercise 2: DAG with shuffle")
  println(s"  Lineage: ${counts.toDebugString}")
  println(s"  Shuffle creates stage boundary")

  // EXERCISE 3: Complex DAG with multiple inputs
  val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5), 2)
  val lettersRDD = sc.parallelize(List("a", "b", "c", "d", "e"), 2)
  val zipped = numbersRDD.zip(lettersRDD)
  println(s"\n✓ Exercise 3: DAG with zip (multiple parents)")
  println(s"  Lineage: ${zipped.toDebugString}")

  // EXERCISE 4: DAG with union (multiple inputs, no shuffle)
  val rddA = sc.parallelize(1 to 5, 2)
  val rddB = sc.parallelize(6 to 10, 2)
  val unioned = rddA.union(rddB)
  println(s"\n✓ Exercise 4: Union (narrow dependency)")
  println(s"  Lineage: ${unioned.toDebugString}")
  println(s"  Partitions: ${unioned.getNumPartitions}")

  // EXERCISE 5: Diamond DAG pattern
  val base = sc.parallelize(1 to 100, 4)
  val branch1 = base.map(_ * 2)
  val branch2 = base.map(_ * 3)
  val rejoined = branch1.zip(branch2).map { case (a, b) => a + b }
  println(s"\n✓ Exercise 5: Diamond pattern (shared parent)")
  println(s"  Lineage: ${rejoined.toDebugString}")

  // EXERCISE 6: Observe stage submission order
  val data = sc.parallelize(1 to 1000, 4)
  val mapped = data.map(x => (x % 10, x))
  val reduced = mapped.reduceByKey(_ + _)  // Shuffle
  val sorted = reduced.sortByKey()          // Another shuffle

  println(s"\n✓ Exercise 6: Multi-stage DAG")
  println(s"  Lineage: ${sorted.toDebugString}")
  println(s"  Result: ${sorted.collect().mkString(", ")}")
  println(s"""
  Stage submission order:
  1. Stage 0: parallelize + map (before first shuffle)
  2. Stage 1: reduceByKey (after first shuffle, before second)
  3. Stage 2: sortByKey (after second shuffle)
  """)

  // EXERCISE 7: Cache to break DAG recomputation
  println(s"\n✓ Exercise 7: Cache breaks recomputation")
  val expensiveRDD = sc.parallelize(1 to 100, 4).map { x =>
    Thread.sleep(1) // Simulate expensive computation
    x * x
  }
  expensiveRDD.cache()
  println(s"  First action (computes): sum = ${expensiveRDD.reduce(_ + _)}")
  println(s"  Second action (from cache): max = ${expensiveRDD.max()}")
  expensiveRDD.unpersist()

  Thread.sleep(3000)
  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 15 COMPLETE!")
  println("=" * 60)
}
