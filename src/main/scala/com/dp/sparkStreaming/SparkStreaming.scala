package com.dp.sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.Properties

/**
 * @author miraj
 */
object SparkStreaming {

  val props = new Properties()

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: DP Adtech parameters <mode> ")
      System.exit(1)
    }
    val Array(jobMode) = args
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    // load a properties file
    props.load(getClass().getClassLoader().getResourceAsStream(jobMode + "/spark-job.properties"));

    val dpEventsTopic = props.getProperty("kafka.topic.dp.events")
    val metadata_broker_list = props.getProperty("metadata.broker.list")

    val topicsSet = dpEventsTopic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> metadata_broker_list)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Split each line into words
    val words = lines.flatMap(_._2.split(" "))

    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

// /mnt/spark/bin/spark-submit --class com.dp.sparkStreaming.SparkStreaming --jars /mnt/git/spark/target/scala-2.10/adtech-assembly-0.1.0-deps.jar nt/git/spark/target/scala-2.10/adtech_2.10-0.1.0.jar dev