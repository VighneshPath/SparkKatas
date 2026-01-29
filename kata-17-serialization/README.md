# Kata 17: Serialization

## Goal
Understand how Spark serializes data and how to optimize it for performance.

## Why Serialization Matters

```
SERIALIZATION = Converting objects to bytes for transmission/storage

WHERE SERIALIZATION HAPPENS IN SPARK:
═══════════════════════════════════════════════════════════════════

    1. SHUFFLE: Data sent between executors
       ┌──────────┐         ┌──────────┐
       │Executor 1│ ──────► │Executor 2│
       │ Objects  │ Bytes   │ Objects  │
       └──────────┘ over    └──────────┘
           ↓       network      ↑
       Serialize           Deserialize

    2. CACHING: Data stored in memory/disk
       ┌──────────┐
       │  Objects │ ──Serialize──► │ Bytes in Cache │
       └──────────┘                └────────────────┘

    3. BROADCAST: Variables sent to all executors
       ┌────────┐
       │ Driver │ ──Serialize──► All Executors
       └────────┘

    4. CLOSURE: Functions sent to executors
       ┌──────────────────┐
       │ map(x => x + 1)  │ ──Serialize──► Executors
       └──────────────────┘
```

## Java vs Kryo Serialization

```
JAVA SERIALIZATION (Default)
═══════════════════════════════════════════════════════════════════

    Pros:
    ✓ Works with any Serializable class
    ✓ No configuration needed
    ✓ Handles complex object graphs

    Cons:
    ✗ SLOW (10x slower than Kryo)
    ✗ LARGE output (2-10x larger than Kryo)
    ✗ High CPU usage

    class Person implements Serializable {
        String name;
        int age;
    }

    Java serializes to: ~150 bytes
    Includes: class metadata, field names, type info


KRYO SERIALIZATION (Recommended)
═══════════════════════════════════════════════════════════════════

    Pros:
    ✓ FAST (10x faster than Java)
    ✓ COMPACT (2-10x smaller than Java)
    ✓ Lower GC pressure

    Cons:
    ✗ Must register classes for best performance
    ✗ Some classes need custom serializers

    Same Person class:
    Kryo serializes to: ~20 bytes
    Just the data, minimal overhead!


ENABLE KRYO:
─────────────────────────────────────────────────────────────────
    spark.serializer = org.apache.spark.serializer.KryoSerializer
    spark.kryo.registrationRequired = false  # true for safety

REGISTER CLASSES:
─────────────────────────────────────────────────────────────────
    spark.kryo.classesToRegister = com.myapp.Person,com.myapp.Order

    // Or programmatically:
    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[Person], classOf[Order]))
```

## Serialization Size Comparison

```
BENCHMARK: Serializing 1 million Person objects
═══════════════════════════════════════════════════════════════════

    case class Person(name: String, age: Int, city: String)

    ┌─────────────────┬────────────────┬─────────────────┐
    │  Serializer     │  Size (MB)     │  Time (ms)      │
    ├─────────────────┼────────────────┼─────────────────┤
    │  Java           │     250 MB     │    5000 ms      │
    │  Kryo (unreg)   │      80 MB     │    1200 ms      │
    │  Kryo (reg)     │      45 MB     │     500 ms      │
    └─────────────────┴────────────────┴─────────────────┘

    Kryo with registration: 5x smaller, 10x faster!
```

## The Closure Serialization Problem

```
PROBLEM: Accidentally capturing large objects in closures
═══════════════════════════════════════════════════════════════════

    class MyProcessor {
        val hugeConfig = loadConfig()  // 100MB object!

        def process(rdd: RDD[String]): RDD[Int] = {
            val threshold = hugeConfig.threshold  // Captures hugeConfig!

            rdd.map { line =>
                if (line.length > threshold) 1 else 0
                // ↑ This closure captures the entire MyProcessor
                //   including hugeConfig (100MB)!
            }
        }
    }

    Result: 100MB sent to EVERY task!


SOLUTION: Extract only what you need
═══════════════════════════════════════════════════════════════════

    class MyProcessor {
        val hugeConfig = loadConfig()

        def process(rdd: RDD[String]): RDD[Int] = {
            val threshold = hugeConfig.threshold  // Extract value
            val localThreshold = threshold        // Local variable

            rdd.map { line =>
                if (line.length > localThreshold) 1 else 0
                // ↑ Only captures localThreshold (8 bytes)!
            }
        }
    }
```

## NotSerializableException

```
COMMON ERROR:
═══════════════════════════════════════════════════════════════════

    org.apache.spark.SparkException: Task not serializable
    Caused by: java.io.NotSerializableException: com.myapp.DatabaseConnection

WHY IT HAPPENS:
─────────────────────────────────────────────────────────────────

    class DataProcessor {
        val dbConnection = new DatabaseConnection()  // NOT Serializable!

        def process(rdd: RDD[Row]): Unit = {
            rdd.foreach { row =>
                dbConnection.insert(row)  // Tries to serialize dbConnection
            }
        }
    }


SOLUTIONS:
─────────────────────────────────────────────────────────────────

    // Solution 1: Create connection per partition
    rdd.foreachPartition { rows =>
        val conn = new DatabaseConnection()  // Created on executor
        rows.foreach(row => conn.insert(row))
        conn.close()
    }

    // Solution 2: Use @transient
    class DataProcessor extends Serializable {
        @transient lazy val dbConnection = new DatabaseConnection()

        def process(rdd: RDD[Row]): Unit = {
            rdd.foreachPartition { rows =>
                rows.foreach(row => dbConnection.insert(row))
            }
        }
    }

    // Solution 3: Use broadcast for read-only data
    val config = spark.sparkContext.broadcast(loadConfig())
    rdd.map(x => config.value.transform(x))
```

## Serialized vs Deserialized Storage

```
STORAGE LEVELS AND SERIALIZATION
═══════════════════════════════════════════════════════════════════

    MEMORY_ONLY:
    ┌────────────────────────────────────────────────────────────┐
    │  JVM Objects in heap                                       │
    │  ┌─────────┐ ┌─────────┐ ┌─────────┐                     │
    │  │ Person  │ │ Person  │ │ Person  │  ...                │
    │  │ obj 1   │ │ obj 2   │ │ obj 3   │                     │
    │  └─────────┘ └─────────┘ └─────────┘                     │
    │                                                            │
    │  + Fast access (no deserialization)                       │
    │  - High memory usage (object overhead)                    │
    │  - GC pressure                                            │
    └────────────────────────────────────────────────────────────┘

    MEMORY_ONLY_SER:
    ┌────────────────────────────────────────────────────────────┐
    │  Serialized byte arrays                                    │
    │  ┌─────────────────────────────────────────────────────┐ │
    │  │ [bytes][bytes][bytes][bytes][bytes][bytes]...       │ │
    │  └─────────────────────────────────────────────────────┘ │
    │                                                            │
    │  + Compact (2-5x less memory)                             │
    │  + Less GC pressure                                       │
    │  - Slower access (deserialization cost)                   │
    └────────────────────────────────────────────────────────────┘


WHEN TO USE SERIALIZED STORAGE:
─────────────────────────────────────────────────────────────────
  - Memory is tight
  - Data is accessed infrequently
  - GC is a bottleneck
  - Large objects with many fields
```

## Custom Kryo Serializers

```scala
// For classes that need special handling
class PersonSerializer extends Serializer[Person] {
    override def write(kryo: Kryo, output: Output, person: Person): Unit = {
        output.writeString(person.name)
        output.writeInt(person.age)
    }

    override def read(kryo: Kryo, input: Input, `type`: Class[Person]): Person = {
        Person(input.readString(), input.readInt())
    }
}

// Register custom serializer
val conf = new SparkConf()
conf.set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)

class MyKryoRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo): Unit = {
        kryo.register(classOf[Person], new PersonSerializer())
    }
}
```

## Best Practices

```
SERIALIZATION CHECKLIST:
═══════════════════════════════════════════════════════════════════

  □ Use Kryo serializer (10x faster than Java)
  □ Register frequently used classes
  □ Avoid capturing large objects in closures
  □ Use broadcast for shared read-only data
  □ Use @transient for non-serializable fields
  □ Create connections per partition, not per record
  □ Consider serialized storage for memory-constrained jobs
  □ Test serialization: rdd.map(identity).count()  // Will fail if not serializable
```

## Instructions

1. **Read** this README - understand serialization impact
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-17-serialization && sbt run`
4. **Experiment** with Java vs Kryo serialization
5. **Check** `solution/Solution.scala` if needed

## Key Configurations

| Config | Default | Description |
|--------|---------|-------------|
| `spark.serializer` | Java | Use KryoSerializer |
| `spark.kryo.registrationRequired` | false | Require class registration |
| `spark.kryo.classesToRegister` | none | Classes to register |
| `spark.kryoserializer.buffer.max` | 64m | Max buffer size |

## Time
~30 minutes

## Next
Continue to [kata-18-broadcast-accumulators](../kata-18-broadcast-accumulators/)
