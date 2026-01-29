import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * KATA 13: Memory Management - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 13: Memory Management - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata13").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  println(s"\n✓ Exercise 1: Memory Config")
  println(s"  spark.driver.memory: ${spark.conf.getOption("spark.driver.memory").getOrElse("default")}")
  println(s"  spark.executor.memory: ${spark.conf.getOption("spark.executor.memory").getOrElse("default")}")
  println(s"  spark.memory.fraction: ${spark.conf.get("spark.memory.fraction", "0.6")}")

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

  val rdd = sc.parallelize(1 to 100000, 4).map(x => (x, "value_" + x))
  rdd.persist(StorageLevel.MEMORY_ONLY)
  rdd.count()
  println(s"\n✓ Exercise 3: Storage level: ${rdd.getStorageLevel}")

  val deserializedRDD = sc.parallelize(1 to 50000).persist(StorageLevel.MEMORY_ONLY)
  val serializedRDD = sc.parallelize(1 to 50000).persist(StorageLevel.MEMORY_ONLY_SER)
  deserializedRDD.count()
  serializedRDD.count()
  println(s"\n✓ Exercise 4:")
  println(s"  Deserialized: ${deserializedRDD.getStorageLevel}")
  println(s"  Serialized: ${serializedRDD.getStorageLevel}")

  println(s"\n✓ Exercise 5: Unpersist")
  rdd.unpersist()
  deserializedRDD.unpersist()
  serializedRDD.unpersist()
  println(s"  After unpersist: ${rdd.getStorageLevel}")

  val lookupTable = Map("a" -> 1, "b" -> 2, "c" -> 3)
  println(s"\n✓ Exercise 6: Broadcast Variable")
  val broadcastVar = sc.broadcast(lookupTable)
  val data = sc.parallelize(List("a", "b", "c", "a", "b"))
  val result = data.map(x => (x, broadcastVar.value.getOrElse(x, 0)))
  result.collect().foreach(println)

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 13 SOLUTION COMPLETE!")
  println("=" * 60)
}
