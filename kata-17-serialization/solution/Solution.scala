import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * KATA 17: Serialization - SOLUTION
 */
object Solution extends App {

  println("=" * 60)
  println("KATA 17: Serialization - SOLUTION")
  println("=" * 60)

  case class Person(name: String, age: Int, city: String)
  val people = (1 to 10000).map(i => Person(s"Person$i", i % 100, s"City${i % 50}"))

  // Java Serialization
  val sparkJava = SparkSession.builder().appName("Kata17-Java").master("local[2]").getOrCreate()
  println(s"\n✓ Exercise 1: Java Serialization")
  val javaStart = System.currentTimeMillis()
  sparkJava.sparkContext.parallelize(people, 4).map(p => (p.city, p.age)).reduceByKey(_ + _).collect()
  println(s"  Time: ${System.currentTimeMillis() - javaStart}ms")
  sparkJava.stop()

  // Kryo Serialization
  val sparkKryo = SparkSession.builder().appName("Kata17-Kryo").master("local[2]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
  println(s"\n✓ Exercise 2: Kryo Serialization")
  val kryoStart = System.currentTimeMillis()
  sparkKryo.sparkContext.parallelize(people, 4).map(p => (p.city, p.age)).reduceByKey(_ + _).collect()
  println(s"  Time: ${System.currentTimeMillis() - kryoStart}ms")
  sparkKryo.stop()

  // Final spark session
  val spark = SparkSession.builder().appName("Kata17").master("local[2]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
  val sc = spark.sparkContext

  // Closure serialization
  println(s"\n✓ Exercise 4: Closure serialization")
  val multiplier = 10
  val result = sc.parallelize(1 to 100, 2).map(x => x * multiplier).collect()
  println(s"  Captured primitive: ${result.take(5).mkString(", ")}...")

  // Serialized storage
  println(s"\n✓ Exercise 5: Storage levels")
  val largeRDD = sc.parallelize(1 to 100000, 4).map(i => (i, s"value_$i"))
  largeRDD.persist(StorageLevel.MEMORY_ONLY)
  largeRDD.count()
  println(s"  MEMORY_ONLY: deserialized=${largeRDD.getStorageLevel.deserialized}")
  largeRDD.unpersist()

  largeRDD.persist(StorageLevel.MEMORY_ONLY_SER)
  largeRDD.count()
  println(s"  MEMORY_ONLY_SER: deserialized=${largeRDD.getStorageLevel.deserialized}")
  largeRDD.unpersist()

  spark.stop()
  println("\n" + "=" * 60)
  println("KATA 17 SOLUTION COMPLETE!")
  println("=" * 60)
}
