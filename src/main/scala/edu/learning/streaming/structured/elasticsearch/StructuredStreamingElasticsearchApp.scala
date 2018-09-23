package edu.learning.streaming.structured.elasticsearch

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object StructuredStreamingElasticsearchApp extends App{

  //creating SparkSession object with Elasticsearch configuration
  val sparkSession = SparkSession.builder()
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "username")
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "password")
    .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
    .config(ConfigurationOptions.ES_PORT, "9200")
    .master("local[*]")
    .appName("sample-structured-streaming")
    .getOrCreate()

  //creating schema for JSON
  val jsonSchema = StructType(
    Seq(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("nationality", StringType, true),
      StructField("skills", ArrayType(StringType, true), true)
    )
  )

  //read from JSON file
  val streamingDF: DataFrame = sparkSession
    .readStream
    .schema(jsonSchema)
    .json("src/main/resources/")

  //writing to elasticsearch
  streamingDF.writeStream
    .outputMode("append")
    .format("org.elasticsearch.spark.sql")
    .option("checkpointLocation", "/home/anuj/temp")
    .start("employee1/personal")
    .awaitTermination()
}
