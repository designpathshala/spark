package com.dp.dptech.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import java.util.Properties
//import org.apache.spark.sql.cassandra.CassandraSQLContext
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

    //    val dpPaymentsTopic = props.getProperty("kafka.topic.dp.payments", "dpPayments")
    val dpEventsTopic = props.getProperty("kafka.topic.dp.events")
    val metadata_broker_list = props.getProperty("metadata.broker.list")
    val spark_cassandra_connection_host = props.getProperty("spark.cassandra.connection.host")

    /**Configures Spark. */
    val conf = new SparkConf(true).
      set("spark.cassandra.connection.host", spark_cassandra_connection_host).
      setAppName("DPKafkaWindowStreaming")

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("checkpoint")

    props.load(getClass.getResourceAsStream("/" + jobMode + "/spark-job.properties"))

    val topicsSet = dpEventsTopic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> metadata_broker_list)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    println("STARTING READING: topics: " + topicsSet + "   Kafka servers: " + metadata_broker_list + "  spark servers: " + spark_cassandra_connection_host)
    println("----------No of lines read: " + lines.count().count())

//    val cc = new CassandraSQLContext(ssc.sparkContext)
//    cc.setKeyspace("ks_dptech")

    lines foreachRDD {
      (dpRdd, time) =>
        println("Running for loop...." + dpRdd.count())
        val sqlContext = SQLContext.getOrCreate(dpRdd.sparkContext)
//        SQLUtilities.runOperations(dpRdd, false, sqlContext, cc, dpEventsTopic, jobMode)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}


