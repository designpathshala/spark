package com.dp.dptech.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._

object Write {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("elasticSearch")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val game = Map("media_type" -> "game", "title" -> "FF VI", "year" -> "1994")
    val book = Map("media_type" -> "book", "title" -> "Harry Potter", "year" -> "2010")
    val cd = Map("media_type" -> "music", "title" -> "Surfing With The Alien")

    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection-{media_type}/doc")
  }
}

