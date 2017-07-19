package com.dp.dptech.realtime


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.Minutes

object TwitterStreaming {
  
  System.setProperty("twitter4j.oauth.consumerKey", "FlRx3d0n8duIQ0UvGeGtTA")
  System.setProperty("twitter4j.oauth.consumerSecret", "DS7TTbxhmQ7oCUlDntpQQRqQllFFOiyNoOMEDD0lA")
  System.setProperty("twitter4j.oauth.accessToken", "1643982224-xTfNpLrARoWKxRh9KtFqc7aoB8KAAHkCcfC5vDk")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "PqkbuBqF3AVskgx1OKgXKOZzV7EMWRmRG0p8hvLQYKs")

  val filters = Array("narendramodi")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TwitterAnalysis").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint(this.getClass.getClassLoader.getResource("")+"/spark/staging")

    val streams = TwitterUtils.createStream(ssc,None,filters)

    val hashTags = streams.flatMap(status => status.getText.split(" ")).filter(_.startsWith("#"))

    
    
    
    //window based taking the hashTags from DStreams
    //Minutes will be Windows length ,Seconds will be Sliding Interval
    //Count the hashtags over last 10 mins
    hashTags.window(Minutes(60),Seconds(10)).countByValue().print()

    //Top hashTags with in the Batch interval of  60 Seconds and counting the HashTags by ReduceByKey Operation
    val top60hashTags = hashTags.map(w => (w,1)).reduceByKeyAndWindow(_+_,Seconds(60))

    //Top hashTags with in the Batch interval of  10 Seconds and counting the HashTags by ReduceByKey Operation
    val top10hashTags = hashTags.map(w => (w,1)).reduceByKeyAndWindow(_+_,Seconds(10))


    //window based Counting values from the given Batch Interval..
    hashTags.countByValueAndWindow(Seconds(10),Seconds(5)).print()



    top60hashTags.foreachRDD{ rdd =>

      val topList = rdd.take(10)
      println("\n Popular topics in last 60 Seconds (%s total) :".format(rdd.count()))
      topList.foreach{case (count,topic) => println("%s (%s tweetes)".format(count,topic))}
    }


    top10hashTags.foreachRDD{ rdd =>

      val topList = rdd.take(10)
      println("\n Popular topics in last 10 Seconds (%s total) :".format(rdd.count()))
      topList.foreach{case (count,topic) => println("%s (%s tweetes)".format(count,topic))}
    }

    //saving top hash Tags in batchInterval of Seconds 60
    //top60hashTags.saveAsTextFiles("hdfs://localhost:8020/user/spark/twitter");
    
    ssc.start()
    ssc.awaitTermination()

  }
}