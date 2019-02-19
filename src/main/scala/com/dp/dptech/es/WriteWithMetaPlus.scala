package com.dp.dptech.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._

/**
 * The metadata used for otp document. In this case, ID with a value of 1 and TTL with a value of 3h
 * The metadata used for muc document. In this case, ID with a value of 2 and VERSION with a value of 23
 * The metadata used for sfo document. In this case, ID with a value of 3
 * The metadata and the documents are assembled into a pair RDD
 * The RDD is saved accordingly using the saveToEsWithMeta method
 */
object WriteWithMetaPlus {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("elasticSearch")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    // metadata for each document
    // note it's not required for them to have the same structure
    import org.elasticsearch.spark.rdd.Metadata._
    
    val otpMeta = Map(ID -> 1, TTL -> "3h")
    val mucMeta = Map(ID -> 2, VERSION -> "23")
    val sfoMeta = Map(ID -> 3)

    val airportsRDD = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))
    airportsRDD.saveToEsWithMeta("airports/2015")
  }
}