import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

/**
 * KATA 17: Serialization
 * Run with: sbt run
 */
object Exercise extends App {

  println("=" * 60)
  println("KATA 17: Serialization")
  println("=" * 60)

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 1: Default Java Serialization
  // ─────────────────────────────────────────────────────────────────

  val sparkJava = SparkSession.builder()
    .appName("Kata17-JavaSerializer")
    .master("local[2]")
    .getOrCreate()

  println(s"\n✓ Exercise 1: Java Serialization")
  println(s"  Serializer: ${sparkJava.conf.get("spark.serializer", "JavaSerializer (default)")}")

  case class Person(name: String, age: Int, city: String)

  val sc = sparkJava.sparkContext
  val people = (1 to 10000).map(i => Person(s"Person$i", i % 100, s"City${i % 50}"))
  val rdd = sc.parallelize(people, 4)

  val javaStart = System.currentTimeMillis()
  rdd.map(p => (p.city, p.age)).reduceByKey(_ + _).collect()
  val javaTime = System.currentTimeMillis() - javaStart
  println(s"  Time with Java serializer: ${javaTime}ms")

  sparkJava.stop()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 2: Kryo Serialization
  // ─────────────────────────────────────────────────────────────────
  // Kryo is faster and more compact
  // ─────────────────────────────────────────────────────────────────

  val sparkKryo = SparkSession.builder()
    .appName("Kata17-KryoSerializer")
    .master("local[2]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  println(s"\n✓ Exercise 2: Kryo Serialization")
  println(s"  Serializer: ${sparkKryo.conf.get("spark.serializer")}")

  val scKryo = sparkKryo.sparkContext
  val rddKryo = scKryo.parallelize(people, 4)

  val kryoStart = System.currentTimeMillis()
  rddKryo.map(p => (p.city, p.age)).reduceByKey(_ + _).collect()
  val kryoTime = System.currentTimeMillis() - kryoStart
  println(s"  Time with Kryo serializer: ${kryoTime}ms")
  println(s"  Speedup: ${javaTime.toDouble / kryoTime}x")

  sparkKryo.stop()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 3: Kryo with registered classes
  // ─────────────────────────────────────────────────────────────────

  val sparkKryoReg = SparkSession.builder()
    .appName("Kata17-KryoRegistered")
    .master("local[2]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.classesToRegister", "Exercise$Person$1")  // Register our class
    .getOrCreate()

  println(s"\n✓ Exercise 3: Kryo with registration")

  val scReg = sparkKryoReg.sparkContext
  val rddReg = scReg.parallelize(people, 4)

  val regStart = System.currentTimeMillis()
  rddReg.map(p => (p.city, p.age)).reduceByKey(_ + _).collect()
  val regTime = System.currentTimeMillis() - regStart
  println(s"  Time with registered classes: ${regTime}ms")

  sparkKryoReg.stop()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 4: Closure serialization
  // ─────────────────────────────────────────────────────────────────
  // Be careful what you capture in closures!
  // ─────────────────────────────────────────────────────────────────

  val spark = SparkSession.builder()
    .appName("Kata17-Closures")
    .master("local[2]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  println(s"\n✓ Exercise 4: Closure serialization")

  val multiplier = 10  // This primitive will be captured

  val scFinal = spark.sparkContext
  val numbers = scFinal.parallelize(1 to 100, 2)

  // Good: capturing primitive
  val result = numbers.map(x => x * multiplier).collect()
  println(s"  Captured primitive works: ${result.take(5).mkString(", ")}...")

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 5: Serialized storage level
  // ─────────────────────────────────────────────────────────────────

  import org.apache.spark.storage.StorageLevel

  println(s"\n✓ Exercise 5: Serialized vs deserialized storage")

  val largeRDD = scFinal.parallelize(1 to 100000, 4).map(i => (i, s"value_$i"))

  // Deserialized storage (default)
  largeRDD.persist(StorageLevel.MEMORY_ONLY)
  largeRDD.count()
  println(s"  MEMORY_ONLY: deserialized=${largeRDD.getStorageLevel.deserialized}")
  largeRDD.unpersist()

  // Serialized storage (more compact)
  largeRDD.persist(StorageLevel.MEMORY_ONLY_SER)
  largeRDD.count()
  println(s"  MEMORY_ONLY_SER: deserialized=${largeRDD.getStorageLevel.deserialized}")
  println(s"  Serialized storage uses less memory but requires deserialization on access")
  largeRDD.unpersist()

  // ─────────────────────────────────────────────────────────────────
  // EXERCISE 6: Testing serialization
  // ─────────────────────────────────────────────────────────────────

  println(s"\n✓ Exercise 6: Testing serialization")
  println(s"  If a class isn't serializable, this would fail:")

  // This works because Person is a case class (serializable)
  val testRDD = scFinal.parallelize(Seq(Person("Test", 25, "NYC")))
  val collected = testRDD.collect()
  println(s"  Serialized and collected: ${collected.head}")

  spark.stop()

  println("\n" + "=" * 60)
  println("KATA 17 COMPLETE!")
  println("=" * 60)
}
