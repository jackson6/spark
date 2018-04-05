package com.buckit.spark.streaming

// Basic Spark imports
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

// Spark SQL Cassandra imports
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

// Spark Streaming + Kafka imports
import kafka.serializer.StringDecoder // this has to come before streaming.kafka import
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka._

// Cassandra Java driver imports
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import scala.collection.JavaConversions._

// Date import for processing logic
import java.util.Date

import com.datastax.spark.connector.mapper.DefaultColumnMapper

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

import scala.util.parsing.json._
import java.util.Calendar


// spark-submit --conf spark.cassandra.connection.host=10.0.0.125 --packages datastax:spark-cassandra-connector:2.0.1-s_2.11,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.1 --class com.buckit.spark.streaming.DirectKafkaImpressionCount target/scala-2.11/spark-impressions-count_2.11-1.0.jar

case class ImpressionData(serial_code: String, company_id: String, created_on: java.util.Date)

object ImpressionData {
    implicit object Mapper extends DefaultColumnMapper[ImpressionData](
        Map("serial_code" -> "serial_code", "company_id" -> "company_id", "created_on" -> "created_on"))
}

object DirectKafkaImpressionCount {
  def main(args: Array[String]) {

    val topics = Set("impressions")
    val brokers = "10.0.0.125:9092"

    // Create context with 2 second batch interval
    val spark: SparkSession = SparkSession.builder().appName("AppName").config("spark.master", "local").getOrCreate()

    //Schema
    val schema = StructType(
        StructField("company_id", StringType) ::
        StructField("created_on", TimestampType  ) ::
        StructField("product_id", StringType) ::
        StructField("views", IntegerType) ::
        Nil
    )

    // load and cash impressions table
    var impressions: RDD[Row] = spark.sparkContext.cassandraTable[ImpressionData]("buckit_analytics", "buckit_impressions")
                    .keyBy(row => (row.serial_code, row.company_id))
                    .map {case (key, value) => (key, 1)}
                    .reduceByKey(_+_)
                    .map { case ((serial_code, company_id), views) => Row(serial_code, company_id, views) }.cache()

    //val impressions = spark.createDataFrame(rdd, schema)
    //df.createOrReplaceTempView("impressions")

    val batchIntervalSeconds = 1


    // Create direct kafka stream with brokers and topics
    def createKafkaStream(ssc: StreamingContext): InputDStream[(String, String)] = {
      val kafkaParams = Map[String, String]("metadata.broker.list" -> "10.0.0.125:9092")
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    def creatingFunc(): StreamingContext = {

      // Create a StreamingContext
      val ssc = new StreamingContext(spark.sparkContext, Seconds(batchIntervalSeconds))

      // Get the impression stream from the source
      val stream = createKafkaStream(ssc)

      //Print the streaming
      stream.print

      stream.foreachRDD(
        rdd => {
          if (rdd.toLocalIterator.nonEmpty) {
            val json =  spark.sqlContext.read.json(rdd.map(x => x._2))
                              .rdd.keyBy(row => (row(1), row(2)))

            val streamRDD: RDD[Row] = json.map {case (key, _) => (key, 1)}
                                          .reduceByKey(_+_)
                                          .map { case ((product_id, company_id), views) => Row(product_id, company_id, views) }

            impressions = impressions.union(streamRDD)
                                  .keyBy(row => (row(1), row(2)))
                                  .mapValues(row => row(3))
                                  .filter {case (key, _) => json.collect() contains key}
                                  .map {case (key, value) => (key, Integer.valueOf(value.toString()))}
                                  .reduceByKey(_+_)
                                  .map { case ((product_id, company_id), views) => Row(product_id, company_id, views) }.cache()



            val df = spark.createDataFrame(impressions, schema)

            df.show()

          }
        }
      )

      // To make sure data is not deleted by the time we query it interactively
      ssc.remember(Minutes(1))
      ssc
    }

    def saveImpressions(rdd: RDD[Row], schema: StructType){
      rdd.saveToCassandra("buckit_analytics", "aggr_impressions", SomeColumns("product_id", "company_id", "views"))
    }

    // Start the computation
    val stopActiveContext = true
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    // Get or create a streaming context.
    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

    // This starts the streaming context in the background.
    ssc.start()

    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
    ssc.awaitTermination()
  }
}
