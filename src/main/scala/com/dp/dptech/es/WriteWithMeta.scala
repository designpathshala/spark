package com.dp.dptech.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._

/**
 * airportsRDD is a key-value pair RDD; it is created from a Seq of tuples
 * The key of each tuple within the Seq represents the id of its associated value/document; 
 * in other words, document otp has id 1, muc 2 and sfo 3
 * 
 * Since airportsRDD is a pair RDD, it has the saveToEsWithMeta method available.
 *  
 * This tells elasticsearch-hadoop to pay special attention to the RDD keys and use them as metadata, 
 * in this case as document ids. If saveToEs would have been used instead, 
 * then elasticsearch-hadoop would consider the RDD tuple, that is both the key and the value, 
 * as part of the document.
 */
object WriteWithMeta {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("elasticSearch")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    airportsRDD.saveToEsWithMeta("airports/2015")
  }
}