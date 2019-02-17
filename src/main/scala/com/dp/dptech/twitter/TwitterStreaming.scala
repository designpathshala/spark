package com.dp.dptech.twitter

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.Minutes

object TwitterStreaming {

  System.setProperty("twitter4j.oauth.consumerKey", "7qEI8NvHy3tv3UQn4h333EjGZ")
  System.setProperty("twitter4j.oauth.consumerSecret", "jFrL4C1tg5roXVdKnhMTuCOmTUjpMHjgdFMI4lMrPTyEt6bcrA")
  System.setProperty("twitter4j.oauth.accessToken", "119322757-DUgYohWk75tGhCGHsSBlEdCPFuKymrbvv8WgaMc9")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "2TqReSia1tXofRiYHU4MF4CiZVoc6fnKSQWC2Qfl1MhME")

  val filters = Array("narendramodi", "modi", "trump", "delhi", "Padmavati")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TwitterAnalysis").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val streams = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = streams.flatMap(status => status.getText.split(" ")).filter(_.startsWith("#"))

    //window based taking the hashTags from DStreams
    //Minutes will be Windows length ,Seconds will be Sliding Interval
    //Count the hashtags over last 10 mins
    hashTags.window(Minutes(10), Seconds(10)).countByValue().print()

    //Top hashTags with in the Batch interval of  60 Seconds and counting the HashTags by ReduceByKey Operation
    val top60hashTags = hashTags.map(w => (w, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))

    //Top hashTags with in the window of 120 sec and interval of  10 Seconds  and counting the HashTags by ReduceByKey Operation
    val top10hashTags = hashTags.map(w => (w, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x+y,Seconds(120), Seconds(10))

     val top10hashTagsOptimized = hashTags.map(w => (w, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x+y, (x: Int, y: Int) => x-y,Seconds(120), Seconds(10))
  
     //window based Counting values from the given Batch Interval..
    hashTags.countByValueAndWindow(Seconds(10), Seconds(5)).print()

    top60hashTags.foreachRDD { rdd =>

      val topList = rdd.take(10)
      println("\n Popular topics in last 10 Seconds (%s total) :".format(rdd.count()))
      topList.foreach { case (count, topic) => println("%s (%s tweetes)".format(count, topic)) }
    }

    top10hashTags.foreachRDD { rdd =>

      val topList = rdd.take(10)
      println("\n Popular topics in last 10 Seconds (%s total) :".format(rdd.count()))
      topList.foreach { case (count, topic) => println("%s (%s tweetes)".format(count, topic)) }
    }

    //saving top hash Tags in batchInterval of Seconds 60
    //    top60hashTags.saveAsTextFiles("hdfs://localhost:8020/user/spark/twitter")

    ssc.start()
    ssc.awaitTermination()

  }
}

//spark-submit --class com.dp.dptech.twitter.TwitterStreaming --jars /usr/lib/hue/designpathshala/spark/dp-spark-assembly-0.1.0-deps.jar /usr/lib/hue/designpathshala/spark/dp-spark_2.10-0.1.0.jar

//If getting credintials issue, check system time
//chkconfig ntpd on
//ntpdate pool.ntp.org
///etc/init.d/ntpd start