package edu.learning.streaming.structured.elasticsearch

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object StructuredStreamingElasticsearchApp extends App{

  private val config = ConfigFactory.load()

  private val master = config.getString("spark.master")
  private val appName  = config.getString("spark.app.name")

  private val pathToJSONResource = config.getString("spark.json.resource.path")

  private val elasticsearchUser = config.getString("spark.elasticsearch.username")
  private val elasticsearchPass = config.getString("spark.elasticsearch.password")
  private val elasticsearchHost = config.getString("spark.elasticsearch.host")
  private val elasticsearchPort = config.getString("spark.elasticsearch.port")

  private val outputMode = config.getString("spark.elasticsearch.output.mode")
  private val destination = config.getString("spark.elasticsearch.data.source")
  private val checkpointLocation = config.getString("spark.elasticsearch.checkpoint.location")
  private val index = config.getString("spark.elasticsearch.index")
  private val docType = config.getString("spark.elasticsearch.doc.type")
  private val indexAndDocType = s"$index/$docType"

  //creating SparkSession object with Elasticsearch configuration
  val sparkSession = SparkSession.builder()
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, elasticsearchUser)
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, elasticsearchPass)
    .config(ConfigurationOptions.ES_NODES, elasticsearchHost)
    .config(ConfigurationOptions.ES_PORT, elasticsearchPort)
    .master(master)
    .appName(appName)
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
    .json(pathToJSONResource)

  //writing to elasticsearch
  streamingDF.writeStream
    .outputMode(outputMode)
    .format(destination)
    .option("checkpointLocation", checkpointLocation)
    .start(indexAndDocType)
    .awaitTermination()
}
