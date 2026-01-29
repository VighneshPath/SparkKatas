name := "kata-01-spark-session"
version := "1.0.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

fork := true
outputStrategy := Some(StdoutOutput)

javaOptions ++= Seq(
  "-Xms512M", "-Xmx2G",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)

// Suppress Spark logs
javaOptions += "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
