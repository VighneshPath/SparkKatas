import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * KATA 13: Memory Management
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 13: Memory Management")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata13").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  // EXERCISE 1: Check memory configuration
  println(s"\n✓ Exercise 1: Memory Config")
  println(s"  spark.driver.memory: ${spark.conf.getOption("spark.driver.memory").getOrElse("default")}")
  println(s"  spark.executor.memory: ${spark.conf.getOption("spark.executor.memory").getOrElse("default")}")
  println(s"  spark.memory.fraction: ${spark.conf.get("spark.memory.fraction", "0.6")}")

  // EXERCISE 2: Understand storage levels
  println(s"\n✓ Exercise 2: Storage Levels")
  val levels = List(
    ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
    ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
    ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER),
    ("DISK_ONLY", StorageLevel.DISK_ONLY)
  )
  levels.foreach { case (name, level) =>
    println(s"  $name: useDisk=${level.useDisk}, useMemory=${level.useMemory}, deserialized=${level.deserialized}")
  }

  // EXERCISE 3: Monitor cached RDD memory usage
  val rdd = sc.parallelize(1 to 100000, 4).map(x => (x, "value_" + x))
  rdd.persist(StorageLevel.MEMORY_ONLY)
  rdd.count() // Trigger caching
  println(s"\n✓ Exercise 3: Check Spark UI at http://localhost:4040 for memory usage")
  println(s"  Storage level: ${rdd.getStorageLevel}")

  // EXERCISE 4: Serialized vs Deserialized storage
  val deserializedRDD = sc.parallelize(1 to 50000).persist(StorageLevel.MEMORY_ONLY)
  val serializedRDD = sc.parallelize(1 to 50000).persist(StorageLevel.MEMORY_ONLY_SER)
  deserializedRDD.count()
  serializedRDD.count()
  println(s"\n✓ Exercise 4:")
  println(s"  Deserialized: ${deserializedRDD.getStorageLevel}")
  println(s"  Serialized: ${serializedRDD.getStorageLevel}")
  println(s"  (Serialized uses less memory but more CPU)")

  // EXERCISE 5: Unpersist to free memory
  println(s"\n✓ Exercise 5: Unpersist")
  // TODO: Understand the importance of unpersisting to free memory
  rdd.unpersist()
  deserializedRDD.unpersist()
  serializedRDD.unpersist()
  println(s"  After unpersist: ${rdd.getStorageLevel}")

  // EXERCISE 6: Broadcast variable (efficient memory sharing)
  val lookupTable = Map("a" -> 1, "b" -> 2, "c" -> 3)
  println(s"\n✓ Exercise 6: Broadcast Variable")
  // TODO: Understand that broadcast variables are sent once per executor, not per task
  val broadcastVar = sc.broadcast(lookupTable)
  val data = sc.parallelize(List("a", "b", "c", "a", "b"))
  val result = data.map(x => (x, broadcastVar.value.getOrElse(x, 0)))
  result.collect().foreach(println)

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 13 COMPLETE!")
  println("=" * 60)
}
