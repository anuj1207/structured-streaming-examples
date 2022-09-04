package edu.learning.streaming.structured.monitoring

import org.apache.spark.sql.streaming.Trigger

object SampleSparkApp extends App{

  import org.apache.spark.sql.functions._

  import org.apache.spark.sql.streaming.StreamingQueryListener
  import org.apache.spark.sql.streaming.StreamingQueryListener._

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.filter(x => x.length >= 2)//.groupBy("value").count()

  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .outputMode("append")
    .format("console").trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

//  lines.observe("input_event", count(lit(1)).as("rc")).writeStream.format("console").start()
//  wordCounts.observe("output_event", count(lit(1)).as("output_rc")).writeStream.format("console").start()

  val myListener = new StreamingQueryListener {

    /**
     * Called when a query is started.
     *
     * @note This is called synchronously with
     *       [[org.apache.spark.sql.streaming.DataStreamWriter `DataStreamWriter.start()`]].
     *       `onQueryStart` calls on all listeners before
     *       `DataStreamWriter.start()` returns the corresponding [[StreamingQuery]].
     *       Do not block this method, as it blocks your query.
     */
    def onQueryStarted(event: QueryStartedEvent): Unit = {}

    /**
     * Called when there is some status update (ingestion rate updated, etc.)
     *
     * @note This method is asynchronous. The status in [[StreamingQuery]] returns the
     *       latest status, regardless of when this method is called. The status of [[StreamingQuery]]
     *       may change before or when you process the event. For example, you may find [[StreamingQuery]]
     *       terminates when processing `QueryProgressEvent`.
     */

    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      println("Query made progress")

      // getting current batch information in json format
      val jsonData = queryProgress.progress.json

      println(s"This is json $jsonData\n and this is the id of stream ${query.id}")
    }

    /**
     * Called when a query is stopped, with or without error.
     */
    def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
  }

  spark.streams.addListener(myListener)
  query.awaitTermination()


}
