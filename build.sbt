name := "structured-streaming-examples"

version := "0.1"

scalaVersion := "2.12.11"

val spark2Ver = "3.3.0"

val typesafeConfig = "com.typesafe" % "config" % "1.3.3"
val spark2Sql = "org.apache.spark" %% "spark-sql" % spark2Ver
val elasticsearchHadoop = "org.elasticsearch" % "elasticsearch-hadoop" % "6.4.1"

libraryDependencies ++= Seq(
  typesafeConfig,
  spark2Sql,
  elasticsearchHadoop
)