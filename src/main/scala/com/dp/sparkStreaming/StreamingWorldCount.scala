package com.text

import java.nio.ByteBuffer
import scala.util.Random
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage._
import org.apache.spark.streaming.{ StreamingContext, Seconds, Minutes, Time }
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import java.nio.ByteBuffer
import scala.util.Random
import org.apache.spark.streaming.Duration

/**
 * @author 
 */
object StreaminWorldCount {

//  val kafkaTopics = "YOUR_KAFKA_TOPIC_1,YOUR_KAFKA_TOPIC_2" // command separated list of topics
//  val kafkaBrokers = "YOUR_KAFKA_BROKER_1:HOST1,YOUR_KAFKA_BROKER_2:HOST2" // comma separated list of broker:host
//  val batchIntervalSeconds = 10
//
//  def createKafkaStream(ssc: StreamingContext): DStream[(String, String)] = {
//    val topicsSet = kafkaTopics.split(",").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)
//    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topicsSet)
//
//    val windowSize = Duration(10000L) // 10 seconds
//    val slidingInterval = Duration(2000L) // 2 seconds
//    val checkpointInterval = Duration(20000L) // 20 seconds
//
//    val checkpointDir = "dbfs://streaming/checkpoint/100"
//
//    // Function to create a new StreamingContext and set it up
//    def creatingFunc(): StreamingContext = {
//      val conf = new SparkConf(true)
//      val sc = new SparkContext(conf)
//      // Create a StreamingContext
//      val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
//
//      // Get the word stream from the source
//      val wordStream = createKafkaStream(ssc).flatMap { event => event._2.split(" ") }.map(x => (x, 1))
//
//      val runningCountStream = wordStream.reduceByKeyAndWindow(
//        (x: Int, y: Int) => x + y,
//        (x: Int, y: Int) => x - y, // Subtract counts going out of window
//        windowSize, slidingInterval, 2,
//        (x: (String, Int)) => x._2 != 0) // Filter all keys with zero counts
//
//      // Checkpoint the dstream so that it can be persisted every 20 seconds. If the stream is not checkpointed, the performance will deteriorate significantly over time and eventually crash.
//      runningCountStream.checkpoint(checkpointInterval)
//
//      // Create temp table at every batch interval
//      runningCountStream.foreachRDD { rdd =>
//        val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
//        sqlContext.createDataFrame(rdd).toDF("word", "count").registerTempTable("batch_word_count")
//
//        /* Trigger a dummy action to execute the DAG. This triggering of action will ensure that 
//       the checkpointing is invoked. If there is no action on the DAG, then checkpointing will not 
//       be invoked and if somebody queries the table after 'n' minutes, it will result in processing 
//       a big lineage of rdds.
//    */
//        rdd.take(1)
//      }
//
//      // To make sure data is not deleted by the time we query it interactively
//      ssc.remember(Minutes(1))
//
//      ssc.checkpoint(checkpointDir)
//
//      println("Creating function called to create new StreamingContext")
//      ssc
//    }
//  }
}
