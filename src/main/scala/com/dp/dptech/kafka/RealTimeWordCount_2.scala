package com.dp.dptech.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import java.util.Properties
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

/**
 * Created by DP on 6/23/16.
 */
object RealTimeWordCount_2 {

  val props = new Properties()

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: DP dptech parameters <mode> ")
      System.exit(1)
    }
    val Array(jobMode) = args

    // load a properties file
    props.load(getClass().getClassLoader().getResourceAsStream(jobMode + "/spark-job.properties"));

    val dpEventsTopic = props.getProperty("kafka.topic.dp.events")
    val metadata_broker_list = props.getProperty("metadata.broker.list")

    /**Configures Spark. */
    val conf = new SparkConf(true).
      setAppName("DPKafkaWindowStreaming")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("checkpoint")

    props.load(getClass.getResourceAsStream("/" + jobMode + "/spark-job.properties"))

    val topicsSet = dpEventsTopic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> metadata_broker_list)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet) map (_._2)

    println("STARTING READING: topics: " + topicsSet + "   Kafka servers: " + metadata_broker_list)
    println("----------No of lines read: " + lines.count().count())

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}


// spark-submit --class com.dp.dptech.twitter.RealTimeWordCount --jars /usr/lib/hue/designpathshala/spark/dp-spark-assembly-0.1.0-deps.jar /usr/lib/hue/designpathshala/spark/dp-spark_2.10-0.1.0.jar prod