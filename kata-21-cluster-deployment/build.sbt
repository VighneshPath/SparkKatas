name := "kata-21-cluster-deployment"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

// For local development, we include Spark dependencies
// For cluster deployment, use "provided" scope:
// libraryDependencies ++= Seq(
//   "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
//   "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
// )

fork := true
javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)
