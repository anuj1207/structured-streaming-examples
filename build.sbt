name := "structured-streaming-examples"

version := "0.1"

scalaVersion := "2.11.8"

val spark2Ver = "2.2.0"

val spark2Sql = "org.apache.spark" %% "spark-sql" % spark2Ver

val elasticsearchHadoop = "org.elasticsearch" % "elasticsearch-hadoop" % "6.4.1"

libraryDependencies ++= Seq(
  spark2Sql,
  elasticsearchHadoop
)