package com.dp.dptech.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import java.util.Properties
import java.io.FileInputStream

/**
 * Created by DP on 6/23/16.
 */
object KafkaConsumer {

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

    val ssc = new StreamingContext(conf, Seconds(10))

    props.load(getClass.getResourceAsStream("/" + jobMode + "/spark-job.properties"))

    val topicsSet = dpEventsTopic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> metadata_broker_list)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    println("STARTING READING: topics: " + topicsSet + "   Kafka servers: " + metadata_broker_list )
    println("----------No of lines read: " + lines.count().count())

    lines foreachRDD {
      (dpRdd, time) =>
        println("Running for loop...." + dpRdd.count())
        dpRdd.foreach(println)
        
    }

    ssc.start()
    ssc.awaitTermination()
  }

}


