package com.dp.dptech.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark        

object WriteCaseClass {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("elasticSearch")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    // define a case class
    case class Trip(departure: String, arrival: String)

    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    EsSpark.saveToEs(rdd, "spark/docs")
    //to indicate to Elasticsearch to use the field id as the document id, 
    //update the RDD configuration (it is also possible to set the property on the SparkConf 
    //though due to its global effect it is discouraged):
    //EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "id"))
  }
}