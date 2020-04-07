name := "movie-data-analysis"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.databricks" %% "spark-xml" % "0.6.0",
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc4"
)