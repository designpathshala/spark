package com.dp.dptech.realtime

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.sql.DataFrame

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

//CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
//CREATE TABLE test.userscore (name text PRIMARY KEY, age int, gender text, score int);

/**
 * Created by DP on 6/23/16.
 */
object KafkaCassandra {

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
    val spark_cassandra_connection_host = props.getProperty("spark.cassandra.connection.host")

    /**Configures Spark. */
    val conf = new SparkConf(true).
      set("spark.cassandra.connection.host", spark_cassandra_connection_host).
      setAppName("DPKafkaWindowStreaming")

    val ssc = new StreamingContext(conf, Seconds(10))
    //    ssc.checkpoint("checkpoint")

    props.load(getClass.getResourceAsStream("/" + jobMode + "/spark-job.properties"))

    val topicsSet = dpEventsTopic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> metadata_broker_list)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    println("STARTING READING: topics: " + topicsSet + "   Kafka servers: " + metadata_broker_list + "  spark servers: " + spark_cassandra_connection_host)
    println("----------No of lines read: " + lines.count().count())

    val connector = CassandraConnector(ssc.sparkContext.getConf)
    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    import sqlContext.implicits._

    lines foreachRDD {
      (dpRdd, time) =>
        println("Running for loop...." + dpRdd.count())
        dpRdd.foreach(println)
        val line = dpRdd.map(x => x._2)
        val objDataFrameRdd = line map { x =>
          new UserScore(x).parse().parseToCC()
        }
        objDataFrameRdd.saveToCassandra("test", "userscore", SomeColumns("name", "age","gender","score"))
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

class UserScore(val line: String) {

  var name: String = null
  var age: Int = 0
  var gender: String = null
  var score: Int = 0

  def parse(): UserScore = {
    val fields = line.split(",")

    {

      name = fields.apply(0)
      try {
        age = fields.apply(1).toInt
      } catch {
        case e: Exception => age = 0
      }
      gender = fields.apply(2)
      try {
        score = fields.apply(3).toInt
      } catch {
        case e: Exception => score = 0
      }
    }
    this
  }

  def parseToCC(): UserEvent = {
    var userEventObj: UserScore = this
    UserEvent(userEventObj.name, userEventObj.age, userEventObj.gender, userEventObj.score)
  }
}

case class UserEvent(name: String, age: Int, gender: String, score: Int)
