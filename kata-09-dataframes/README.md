# Kata 09: DataFrames

## Goal
Understand DataFrames - Spark's structured data abstraction with schema and optimizations.
Master the DataFrame API, understand schema management, learn when to use Row vs case classes,
and become proficient with SQL interoperability.

## What is a DataFrame?

A DataFrame is a distributed collection of data organized into named columns. It is conceptually
equivalent to a table in a relational database or a data frame in R/Python, but with richer
optimizations under the hood. DataFrames are the primary abstraction in Spark SQL.

```
═══════════════════════════════════════════════════════════════════════════════════════
                              DataFrame CONCEPTUAL MODEL
═══════════════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                              DataFrame                                          │
  │  ┌─────────────────────────────────────────────────────────────────────────┐   │
  │  │                             SCHEMA                                       │   │
  │  │   StructType(                                                            │   │
  │  │     StructField("id", IntegerType, nullable = false),                    │   │
  │  │     StructField("name", StringType, nullable = true),                    │   │
  │  │     StructField("age", IntegerType, nullable = true),                    │   │
  │  │     StructField("salary", DoubleType, nullable = true)                   │   │
  │  │   )                                                                      │   │
  │  └─────────────────────────────────────────────────────────────────────────┘   │
  │                                                                                 │
  │  ┌─────────────────────────────────────────────────────────────────────────┐   │
  │  │                              DATA (Rows)                                 │   │
  │  │   ┌───────┬───────────┬───────┬───────────┐                             │   │
  │  │   │  id   │   name    │  age  │  salary   │                             │   │
  │  │   ├───────┼───────────┼───────┼───────────┤                             │   │
  │  │   │   1   │  "Alice"  │  30   │  75000.0  │  ← Row 0                    │   │
  │  │   │   2   │  "Bob"    │  25   │  65000.0  │  ← Row 1                    │   │
  │  │   │   3   │  "Carol"  │  35   │  85000.0  │  ← Row 2                    │   │
  │  │   │  ...  │    ...    │  ...  │    ...    │                             │   │
  │  │   └───────┴───────────┴───────┴───────────┘                             │   │
  │  └─────────────────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────────────────┘

  KEY INSIGHT: Schema enables Catalyst optimization & Tungsten memory efficiency
═══════════════════════════════════════════════════════════════════════════════════════
```

## RDD vs DataFrame vs Dataset

```
═══════════════════════════════════════════════════════════════════════════════════════
                           COMPARISON: RDD vs DataFrame vs Dataset
═══════════════════════════════════════════════════════════════════════════════════════

                    RDD                   DataFrame              Dataset[T]
                ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
  Type Safety   │ Compile-time │      │  Runtime     │      │ Compile-time │
                │    ✓         │      │    ✗         │      │    ✓         │
                ├──────────────┤      ├──────────────┤      ├──────────────┤
  Optimization  │ None         │      │ Catalyst     │      │ Catalyst     │
                │    ✗         │      │    ✓         │      │    ✓         │
                ├──────────────┤      ├──────────────┤      ├──────────────┤
  Schema        │ None         │      │ Yes (named)  │      │ Yes (typed)  │
                │    ✗         │      │    ✓         │      │    ✓         │
                ├──────────────┤      ├──────────────┤      ├──────────────┤
  Serialization │ Java/Kryo    │      │ Tungsten     │      │ Tungsten     │
                │   (slow)     │      │   (fast)     │      │   (fast)     │
                ├──────────────┤      ├──────────────┤      ├──────────────┤
  API Style     │ Functional   │      │ Declarative  │      │ Both         │
                │  map/filter  │      │  SQL/DSL     │      │              │
                └──────────────┘      └──────────────┘      └──────────────┘

  RELATIONSHIP:
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  DataFrame = Dataset[Row]                                               │
  │                                                                         │
  │  In Scala: type DataFrame = Dataset[Row]                                │
  │                                                                         │
  │  Dataset[Person] ──.toDF()──> DataFrame ──.as[Person]──> Dataset[Person]│
  └─────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## DataFrame Architecture

```
                         YOUR CODE
                             │
                             ▼
                   ┌─────────────────────┐
                   │    DataFrame API    │  df.select("name").filter($"age" > 20)
                   │    or SQL Query     │  spark.sql("SELECT name WHERE age > 20")
                   └──────────┬──────────┘
                              │
              ┌───────────────┴───────────────┐
              │                               │
              ▼                               ▼
    ┌─────────────────┐             ┌─────────────────┐
    │  Unresolved     │             │   Parsed SQL    │
    │  Logical Plan   │             │      AST        │
    └────────┬────────┘             └────────┬────────┘
              │                               │
              └───────────┬───────────────────┘
                          │
                          ▼
                ┌─────────────────────┐
                │      Analyzer       │  Resolves columns, tables, functions
                │  (Catalog Lookup)   │  Validates types
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │  Catalyst Optimizer │  Predicate pushdown, column pruning
                │  (Logical Rules)    │  Constant folding, join reordering
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │  Physical Planner   │  Choose join strategy, scan method
                │  (Cost-Based)       │  Generate SparkPlan
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │  Tungsten Engine    │  Whole-stage code generation
                │  (Code Generation)  │  Memory-efficient UnsafeRow
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │        RDD          │  Actual distributed execution
                │   (Execution)       │
                └─────────────────────┘
```

## Schema: The Heart of DataFrames

Schema defines the structure of your data. Understanding schema is crucial for working with DataFrames.

### Schema Structure

```scala
import org.apache.spark.sql.types._

// Schema is built from StructType containing StructFields
val schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType, nullable = true),
  StructField("age", IntegerType, nullable = true),
  StructField("address", StructType(Seq(        // Nested structure
    StructField("street", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("zip", StringType, nullable = true)
  )), nullable = true),
  StructField("tags", ArrayType(StringType), nullable = true),  // Array
  StructField("metadata", MapType(StringType, StringType), nullable = true)  // Map
))
```

### Available Data Types

```
═══════════════════════════════════════════════════════════════════════════════════════
                              SPARK SQL DATA TYPES
═══════════════════════════════════════════════════════════════════════════════════════

  NUMERIC TYPES:
  ┌─────────────────┬─────────────────────┬─────────────────────────────────────────┐
  │ Spark Type      │ Scala Type          │ Range / Notes                           │
  ├─────────────────┼─────────────────────┼─────────────────────────────────────────┤
  │ ByteType        │ Byte                │ -128 to 127                             │
  │ ShortType       │ Short               │ -32,768 to 32,767                       │
  │ IntegerType     │ Int                 │ -2^31 to 2^31-1                         │
  │ LongType        │ Long                │ -2^63 to 2^63-1                         │
  │ FloatType       │ Float               │ 32-bit IEEE 754                         │
  │ DoubleType      │ Double              │ 64-bit IEEE 754                         │
  │ DecimalType     │ java.math.BigDecimal│ Arbitrary precision (for money!)        │
  └─────────────────┴─────────────────────┴─────────────────────────────────────────┘

  STRING & BINARY:
  ┌─────────────────┬─────────────────────┬─────────────────────────────────────────┐
  │ StringType      │ String              │ UTF-8 encoded                           │
  │ BinaryType      │ Array[Byte]         │ Raw bytes                               │
  └─────────────────┴─────────────────────┴─────────────────────────────────────────┘

  DATE & TIME:
  ┌─────────────────┬─────────────────────┬─────────────────────────────────────────┐
  │ DateType        │ java.sql.Date       │ Date without time                       │
  │ TimestampType   │ java.sql.Timestamp  │ Date + time with microseconds           │
  └─────────────────┴─────────────────────┴─────────────────────────────────────────┘

  BOOLEAN:
  ┌─────────────────┬─────────────────────┬─────────────────────────────────────────┐
  │ BooleanType     │ Boolean             │ true / false                            │
  └─────────────────┴─────────────────────┴─────────────────────────────────────────┘

  COMPLEX TYPES:
  ┌─────────────────┬─────────────────────┬─────────────────────────────────────────┐
  │ ArrayType       │ Seq[T]              │ ArrayType(elementType, containsNull)    │
  │ MapType         │ Map[K, V]           │ MapType(keyType, valueType, valueNull)  │
  │ StructType      │ Row / case class    │ Nested structure                        │
  └─────────────────┴─────────────────────┴─────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

## Creating DataFrames

### Method 1: From Collections with toDF()

```scala
import spark.implicits._

// Simple case - Spark infers types
val df = Seq(
  ("Alice", 30, 75000.0),
  ("Bob", 25, 65000.0),
  ("Carol", 35, 85000.0)
).toDF("name", "age", "salary")

// Result:
// +-----+---+-------+
// | name|age| salary|
// +-----+---+-------+
// |Alice| 30|75000.0|
// |  Bob| 25|65000.0|
// |Carol| 35|85000.0|
// +-----+---+-------+
```

### Method 2: From Case Classes (Recommended)

```scala
case class Employee(name: String, age: Int, salary: Double, department: String)

val employees = Seq(
  Employee("Alice", 30, 75000.0, "Engineering"),
  Employee("Bob", 25, 65000.0, "Marketing"),
  Employee("Carol", 35, 85000.0, "Engineering")
)

val df = employees.toDF()
// Schema automatically inferred from case class
// Column names match field names
```

### Method 3: From RDD with Explicit Schema

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

// Define schema explicitly
val schema = StructType(Seq(
  StructField("name", StringType, nullable = true),
  StructField("age", IntegerType, nullable = true),
  StructField("salary", DoubleType, nullable = true)
))

// Create RDD of Rows
val rowRDD = spark.sparkContext.parallelize(Seq(
  Row("Alice", 30, 75000.0),
  Row("Bob", 25, 65000.0),
  Row("Carol", 35, 85000.0)
))

// Create DataFrame
val df = spark.createDataFrame(rowRDD, schema)
```

### Method 4: Reading from External Sources

```scala
// CSV with schema inference
val csvDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("path/to/data.csv")

// CSV with explicit schema (recommended for production)
val csvDFWithSchema = spark.read
  .option("header", "true")
  .schema(schema)
  .csv("path/to/data.csv")

// Parquet (schema embedded in file)
val parquetDF = spark.read.parquet("path/to/data.parquet")

// JSON
val jsonDF = spark.read.json("path/to/data.json")

// JDBC
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://host:5432/db")
  .option("dbtable", "employees")
  .option("user", "username")
  .option("password", "password")
  .load()
```

## Key DataFrame Operations

### Selection and Projection

```scala
import org.apache.spark.sql.functions._

// Select columns
df.select("name", "age")
df.select($"name", $"age")
df.select(col("name"), col("age"))

// Select with expressions
df.select($"name", $"salary" * 1.1 as "raised_salary")

// Select all columns plus new ones
df.select($"*", ($"salary" / 12) as "monthly_salary")

// Drop columns
df.drop("temporary_column")

// Rename columns
df.withColumnRenamed("name", "employee_name")

// Add/modify columns
df.withColumn("bonus", $"salary" * 0.1)
df.withColumn("age", $"age" + 1)  // Overwrites existing
```

### Filtering

```scala
// Basic filtering
df.filter($"age" > 25)
df.filter($"department" === "Engineering")  // Note: === not ==
df.where($"salary" >= 70000)  // where is alias for filter

// Multiple conditions
df.filter($"age" > 25 && $"department" === "Engineering")
df.filter($"age" > 25 || $"salary" > 80000)

// Negation
df.filter(!($"department" === "Marketing"))

// NULL handling
df.filter($"manager".isNull)
df.filter($"manager".isNotNull)

// String operations
df.filter($"name".startsWith("A"))
df.filter($"name".endsWith("e"))
df.filter($"name".contains("lic"))
df.filter($"name".like("%lic%"))
df.filter($"name".rlike("^A.*"))  // Regex

// IN clause
df.filter($"department".isin("Engineering", "Marketing"))
```

### Aggregations

```scala
// Simple aggregations
df.count()
df.select(count("*"), countDistinct("department"))
df.select(sum("salary"), avg("salary"), max("age"), min("age"))

// Group by
df.groupBy("department").count()
df.groupBy("department").agg(
  count("*") as "employee_count",
  avg("salary") as "avg_salary",
  max("salary") as "max_salary",
  min("salary") as "min_salary"
)

// Multiple grouping columns
df.groupBy("department", "level").agg(avg("salary"))

// Cube and Rollup (for OLAP-style aggregations)
df.cube("department", "level").agg(sum("salary"))
df.rollup("department", "level").agg(sum("salary"))
```

### Sorting

```scala
// Sort ascending (default)
df.orderBy("age")
df.orderBy($"age")
df.sort("age")

// Sort descending
df.orderBy($"age".desc)
df.orderBy(desc("age"))

// Multiple sort columns
df.orderBy($"department".asc, $"salary".desc)

// Null handling in sort
df.orderBy($"manager".asc_nulls_first)
df.orderBy($"manager".asc_nulls_last)
```

### Joins

```scala
val employees = spark.createDataFrame(Seq(
  (1, "Alice", 101),
  (2, "Bob", 102),
  (3, "Carol", 101)
)).toDF("emp_id", "name", "dept_id")

val departments = spark.createDataFrame(Seq(
  (101, "Engineering"),
  (102, "Marketing"),
  (103, "Finance")
)).toDF("dept_id", "dept_name")

// Inner join (default)
employees.join(departments, "dept_id")
employees.join(departments, Seq("dept_id"), "inner")

// Left outer join
employees.join(departments, Seq("dept_id"), "left")

// Right outer join
employees.join(departments, Seq("dept_id"), "right")

// Full outer join
employees.join(departments, Seq("dept_id"), "full")

// Cross join (Cartesian product)
employees.crossJoin(departments)

// Join with different column names
employees.join(departments, employees("dept_id") === departments("dept_id"))

// Anti join (rows in left that don't match right)
employees.join(departments, Seq("dept_id"), "left_anti")

// Semi join (rows in left that match right, no right columns)
employees.join(departments, Seq("dept_id"), "left_semi")
```

## Row vs Case Class: When to Use Each

```
═══════════════════════════════════════════════════════════════════════════════════════
                              ROW vs CASE CLASS
═══════════════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                                   Row                                           │
  │  ─────────────────────────────────────────────────────────────────────────────  │
  │  PROS:                                                                          │
  │  • Flexible - schema can change at runtime                                      │
  │  • Works with any DataFrame                                                     │
  │  • Required when reading from external sources with unknown schema              │
  │                                                                                 │
  │  CONS:                                                                          │
  │  • No compile-time type safety                                                  │
  │  • Access by index (row.getInt(0)) or cast (row.getAs[String]("name"))         │
  │  • Runtime errors if types don't match                                          │
  │                                                                                 │
  │  USE WHEN:                                                                      │
  │  • Schema is dynamic or unknown at compile time                                 │
  │  • Interoperating with external data sources                                    │
  │  • Writing generic/reusable code                                                │
  └─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────────────┐
  │                              Case Class (Dataset)                               │
  │  ─────────────────────────────────────────────────────────────────────────────  │
  │  PROS:                                                                          │
  │  • Compile-time type safety                                                     │
  │  • IDE auto-completion                                                          │
  │  • Cleaner, more readable code                                                  │
  │  • Catches errors at compile time, not runtime                                  │
  │                                                                                 │
  │  CONS:                                                                          │
  │  • Schema must be known at compile time                                         │
  │  • Slight overhead for encoding/decoding                                        │
  │                                                                                 │
  │  USE WHEN:                                                                      │
  │  • You know the schema at compile time                                          │
  │  • Type safety is important                                                     │
  │  • Working within a typed codebase                                              │
  └─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════
```

### Row Access Examples

```scala
// Creating a Row
val row = Row("Alice", 30, 75000.0)

// Access by index (type must match!)
val name: String = row.getString(0)
val age: Int = row.getInt(1)
val salary: Double = row.getDouble(2)

// Access with getAs (more flexible)
val name2: String = row.getAs[String]("name")
val age2: Int = row.getAs[Int]("age")

// Get value as Any
val anyValue: Any = row.get(0)

// Check for null
if (!row.isNullAt(1)) {
  val age = row.getInt(1)
}

// Row in DataFrame operations
df.map { row =>
  val name = row.getAs[String]("name")
  val salary = row.getAs[Double]("salary")
  s"$name earns $salary"
}
```

### Case Class Examples (Dataset)

```scala
case class Employee(name: String, age: Int, salary: Double)

// Create Dataset from case class
val ds: Dataset[Employee] = Seq(
  Employee("Alice", 30, 75000.0),
  Employee("Bob", 25, 65000.0)
).toDS()

// Type-safe operations
ds.map(emp => emp.copy(salary = emp.salary * 1.1))
ds.filter(emp => emp.age > 25)

// Access fields directly
ds.map(_.name)  // Compile-time checked!

// Convert between DataFrame and Dataset
val df: DataFrame = ds.toDF()
val ds2: Dataset[Employee] = df.as[Employee]
```

## SQL Interoperability

DataFrames integrate seamlessly with SQL. You can mix DataFrame API and SQL freely.

### Registering Tables

```scala
// Create temporary view (session-scoped)
df.createOrReplaceTempView("employees")

// Create global temporary view (application-scoped)
df.createOrReplaceGlobalTempView("global_employees")
// Access with: global_temp.global_employees

// Check if view exists
spark.catalog.tableExists("employees")

// List all tables
spark.catalog.listTables().show()

// Drop view
spark.catalog.dropTempView("employees")
```

### Executing SQL

```scala
// Simple SQL
val result = spark.sql("SELECT name, salary FROM employees WHERE age > 25")

// Complex SQL
val complexResult = spark.sql("""
  SELECT
    department,
    COUNT(*) as emp_count,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary
  FROM employees
  GROUP BY department
  HAVING COUNT(*) > 5
  ORDER BY avg_salary DESC
""")

// SQL with joins
spark.sql("""
  SELECT e.name, e.salary, d.dept_name
  FROM employees e
  JOIN departments d ON e.dept_id = d.dept_id
  WHERE d.dept_name = 'Engineering'
""")

// Using SQL functions
spark.sql("""
  SELECT
    name,
    UPPER(name) as upper_name,
    CONCAT(name, ' - ', department) as full_desc,
    CASE
      WHEN salary > 80000 THEN 'High'
      WHEN salary > 60000 THEN 'Medium'
      ELSE 'Low'
    END as salary_band
  FROM employees
""")
```

### Mixing DataFrame API and SQL

```scala
// Start with DataFrame API
val filtered = df.filter($"age" > 25)

// Register and use SQL
filtered.createOrReplaceTempView("filtered_employees")
val sqlResult = spark.sql("SELECT * FROM filtered_employees WHERE salary > 70000")

// Continue with DataFrame API
val finalResult = sqlResult.groupBy("department").agg(avg("salary"))
```

## Common Built-in Functions

```scala
import org.apache.spark.sql.functions._

// String functions
df.select(
  upper($"name"),
  lower($"name"),
  length($"name"),
  trim($"name"),
  concat($"first_name", lit(" "), $"last_name"),
  substring($"name", 1, 3),
  regexp_replace($"phone", "-", ""),
  split($"tags", ",")
)

// Date functions
df.select(
  current_date(),
  current_timestamp(),
  date_add($"start_date", 30),
  datediff($"end_date", $"start_date"),
  year($"date"), month($"date"), dayofmonth($"date"),
  date_format($"date", "yyyy-MM-dd")
)

// Numeric functions
df.select(
  round($"salary", 2),
  floor($"salary"),
  ceil($"salary"),
  abs($"balance"),
  sqrt($"value"),
  pow($"base", $"exponent")
)

// Conditional functions
df.select(
  when($"age" > 30, "Senior").otherwise("Junior") as "level",
  coalesce($"preferred_name", $"name") as "display_name",
  nullif($"value", 0) as "value_or_null"
)

// Aggregate functions
df.groupBy("department").agg(
  count("*"),
  countDistinct("name"),
  sum("salary"),
  avg("salary"),
  max("salary"),
  min("salary"),
  first("name"),
  last("name"),
  collect_list("name"),
  collect_set("name")
)

// Window functions
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("department").orderBy($"salary".desc)

df.select(
  $"*",
  rank().over(windowSpec) as "rank",
  dense_rank().over(windowSpec) as "dense_rank",
  row_number().over(windowSpec) as "row_num",
  lag($"salary", 1).over(windowSpec) as "prev_salary",
  lead($"salary", 1).over(windowSpec) as "next_salary",
  sum($"salary").over(windowSpec) as "running_total"
)
```

## Common Pitfalls and Solutions

### Pitfall 1: Column Ambiguity in Joins

```scala
// WRONG: Ambiguous column reference after join
val joined = employees.join(departments, "dept_id")
joined.select("dept_id")  // Works, but...
joined.select(employees("dept_id"))  // Might fail!

// RIGHT: Use the join column or alias
val joined = employees.join(departments, Seq("dept_id"))
// Or alias the DataFrames
val e = employees.as("e")
val d = departments.as("d")
e.join(d, $"e.dept_id" === $"d.dept_id")
 .select($"e.name", $"d.dept_name")
```

### Pitfall 2: Case Sensitivity

```scala
// Spark SQL is case-insensitive by default for column names
df.select("NAME")  // Works even if column is "name"
df.select("Name")  // Also works

// But case matters in case classes!
case class Person(Name: String, Age: Int)
df.as[Person]  // Will fail if columns are lowercase!

// Solution: Match case or rename columns
df.withColumnRenamed("name", "Name").as[Person]
```

### Pitfall 3: Null Handling

```scala
// WRONG: This won't filter nulls as expected
df.filter($"age" =!= 30)  // NULL rows are NOT included!

// RIGHT: Handle nulls explicitly
df.filter($"age" =!= 30 || $"age".isNull)
df.filter($"age".isNotNull && $"age" =!= 30)

// NULL-safe equality
df.filter($"age" <=> 30)  // Returns false for NULL = 30, not NULL
```

### Pitfall 4: Schema Inference Performance

```scala
// SLOW: Schema inference reads data twice
val df = spark.read.option("inferSchema", "true").csv("huge_file.csv")

// FAST: Provide schema explicitly
val schema = StructType(...)
val df = spark.read.schema(schema).csv("huge_file.csv")
```

### Pitfall 5: Collect on Large DataFrames

```scala
// DANGEROUS: Can cause OOM
val allData = hugeDF.collect()  // Brings ALL data to driver!

// BETTER: Limit first or use sampling
val sample = hugeDF.limit(1000).collect()
val sample = hugeDF.sample(0.01).collect()

// For iteration, use foreach or foreachPartition
hugeDF.foreach(row => process(row))
```

## Best Practices

### 1. Always Define Schema for Production

```scala
// Development: OK to infer
val df = spark.read.option("inferSchema", "true").csv(path)

// Production: Always define schema
val schema = StructType(Seq(
  StructField("id", LongType, nullable = false),
  StructField("name", StringType, nullable = true),
  StructField("amount", DecimalType(10, 2), nullable = true)
))
val df = spark.read.schema(schema).csv(path)
```

### 2. Use Select Early to Reduce Data

```scala
// Reduce columns early in the pipeline
val result = df
  .select("needed_col1", "needed_col2")  // Prune early!
  .filter($"needed_col1" > 100)
  .groupBy("needed_col2")
  .count()
```

### 3. Prefer Built-in Functions over UDFs

```scala
// SLOW: UDF
val myUpper = udf((s: String) => s.toUpperCase)
df.select(myUpper($"name"))

// FAST: Built-in function
df.select(upper($"name"))
```

### 4. Cache Wisely

```scala
// Cache when DataFrame is reused multiple times
val expensiveDF = df
  .join(otherDF, "key")
  .groupBy("category")
  .agg(sum("amount"))
  .cache()  // Cache before multiple uses

val report1 = expensiveDF.filter($"category" === "A")
val report2 = expensiveDF.filter($"category" === "B")

// Don't forget to unpersist when done
expensiveDF.unpersist()
```

### 5. Partition Wisely for Large Datasets

```scala
// Repartition for parallelism (causes shuffle)
val repartitioned = df.repartition(100)

// Repartition by column (useful before joins/aggregations)
val repartitioned = df.repartition($"key")

// Coalesce to reduce partitions (no shuffle)
val coalesced = df.coalesce(10)
```

## Instructions

1. **Read** this README thoroughly
2. **Open** `src/Exercise.scala` and complete the TODOs
3. **Run**: `cd kata-09-dataframes && sbt run`
4. **Check** `solution/Solution.scala` if needed

## Time
~45 minutes

## Next
Continue to [kata-10-catalyst-optimizer](../kata-10-catalyst-optimizer/)
