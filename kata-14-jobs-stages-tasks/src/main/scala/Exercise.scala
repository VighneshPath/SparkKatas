import org.apache.spark.sql.SparkSession

/**
 * KATA 14: Jobs, Stages, and Tasks
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 14: Jobs, Stages, and Tasks")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata14").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  println("""
  SPARK EXECUTION HIERARCHY:

  Application
      └── Job (triggered by action)
            └── Stage (separated by shuffle)
                  └── Task (one per partition)

  Example: wordCount.reduceByKey().collect()

  Job 0 (collect)
    ├── Stage 0: map() - 4 tasks (4 partitions)
    │     [No shuffle yet]
    └── Stage 1: reduceByKey() + collect() - 4 tasks
          [After shuffle]
  """)

  // EXERCISE 1: Count jobs - each action = 1 job
  val rdd = sc.parallelize(1 to 100, 4)
  println(s"\n✓ Exercise 1: How many jobs will this create?")
  rdd.count()      // Job 1
  rdd.collect()    // Job 2
  rdd.reduce(_ + _) // Job 3
  println(s"  Answer: 3 jobs (one per action)")

  // EXERCISE 2: Count stages - shuffles create stage boundaries
  val words = sc.parallelize(List("a", "b", "a", "c", "b", "a"), 2)
  val wordCounts = words.map(w => (w, 1)).reduceByKey(_ + _)
  println(s"\n✓ Exercise 2: Stages in word count")
  println(s"  Lineage: ${wordCounts.toDebugString}")
  println(s"  Stages: 2 (map is Stage 0, reduceByKey shuffle starts Stage 1)")

  // EXERCISE 3: Count tasks - one task per partition per stage
  val data = sc.parallelize(1 to 1000, 4)
  val result = data.map(_ * 2).filter(_ % 3 == 0).reduce(_ + _)
  println(s"\n✓ Exercise 3: Tasks")
  println(s"  4 partitions, narrow transforms (map, filter) = 4 tasks")
  println(s"  reduce = final aggregation")

  // EXERCISE 4: Multiple shuffles = multiple stages
  val pairs = sc.parallelize(List(("a", 1), ("b", 2), ("a", 3), ("b", 4)), 2)
  val aggregated = pairs.reduceByKey(_ + _)  // Shuffle 1
  val sorted = aggregated.sortByKey()         // Shuffle 2
  println(s"\n✓ Exercise 4: Multiple shuffles")
  println(s"  Lineage: ${sorted.toDebugString}")
  // TODO: Understand how to count stages from lineage (count ShuffledRDD + 1)
  val numStages = 3  // Stage 0 (map), Stage 1 (reduceByKey shuffle), Stage 2 (sortByKey shuffle)
  println(s"  Number of stages: $numStages")

  // EXERCISE 5: View job/stage/task info
  println(s"\n✓ Exercise 5: Check Spark UI")
  println(s"  Open http://localhost:4040 to see:")
  println(s"  - Jobs tab: all jobs")
  println(s"  - Stages tab: stages per job")
  println(s"  - Click a stage to see tasks")

  // EXERCISE 6: Trigger a complex job
  val complexRdd = sc.parallelize(1 to 10000, 8)
    .map(x => (x % 10, x))
    .reduceByKey(_ + _)
    .map { case (k, v) => (k, v * 2) }
    .sortByKey()

  println(s"\n✓ Exercise 6: Complex job")
  println(s"  Result: ${complexRdd.collect().mkString(", ")}")
  println(s"  Lineage: ${complexRdd.toDebugString}")

  // Keep alive briefly for UI inspection
  println(s"\n  Spark UI available at http://localhost:4040 for 5 seconds...")
  Thread.sleep(5000)

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 14 COMPLETE!")
  println("=" * 60)
}
