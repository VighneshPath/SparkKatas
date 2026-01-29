import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * KATA 08: Caching and Persistence - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 08: Caching and Persistence - SOLUTION")
  println("=" * 60)

  val spark = SparkSession.builder().appName("Kata08").master("local[4]").getOrCreate()
  val sc = spark.sparkContext

  // EXERCISE 1 - Without caching
  var computeCount = 0
  val expensiveRDD = sc.parallelize(1 to 10, 2).map { x =>
    computeCount += 1
    Thread.sleep(10)
    x * x
  }
  println(s"\n✓ Exercise 1: Without caching")
  computeCount = 0
  expensiveRDD.reduce(_ + _)
  println(s"  Compute count after first action: $computeCount")
  expensiveRDD.count()
  println(s"  Compute count after second action: $computeCount")

  // EXERCISE 2 - With caching
  computeCount = 0
  val cachedRDD = sc.parallelize(1 to 10, 2).map { x =>
    computeCount += 1
    Thread.sleep(10)
    x * x
  }
  cachedRDD.cache()
  println(s"\n✓ Exercise 2: With caching")
  cachedRDD.reduce(_ + _)
  println(s"  Compute count after first action: $computeCount")
  cachedRDD.count()
  println(s"  Compute count after second action: $computeCount")

  // EXERCISE 3
  val storageLevel = cachedRDD.getStorageLevel
  println(s"\n✓ Exercise 3: Storage level = $storageLevel")

  // EXERCISE 4
  val memAndDiskRDD = sc.parallelize(1 to 100, 4)
  memAndDiskRDD.persist(StorageLevel.MEMORY_AND_DISK)
  memAndDiskRDD.count()
  println(s"\n✓ Exercise 4: ${memAndDiskRDD.getStorageLevel}")

  // EXERCISE 5
  cachedRDD.unpersist()
  memAndDiskRDD.unpersist()
  println(s"\n✓ Exercise 5: After unpersist = ${cachedRDD.getStorageLevel}")

  // EXERCISE 6
  val baseRDD = sc.parallelize(1 to 1000, 4).map(_ * 2).filter(_ % 3 == 0)
  baseRDD.cache()
  println(s"\n✓ Exercise 6: Sum=${baseRDD.reduce(_ + _)}, Max=${baseRDD.max()}, Count=${baseRDD.count()}")
  baseRDD.unpersist()

  // EXERCISE 7
  val largeRDD = sc.parallelize(1 to 10000, 4).map(x => (x, "value_" + x))
  largeRDD.persist(StorageLevel.MEMORY_ONLY_SER)
  largeRDD.count()
  println(s"\n✓ Exercise 7: ${largeRDD.getStorageLevel}, deserialized=${largeRDD.getStorageLevel.deserialized}")
  largeRDD.unpersist()

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 08 SOLUTION COMPLETE!")
  println("=" * 60)
}
