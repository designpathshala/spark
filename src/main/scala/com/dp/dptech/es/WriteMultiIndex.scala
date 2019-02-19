package com.dp.dptech.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._

object WriteMultipleIndex {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("elasticSearch")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
  }
}