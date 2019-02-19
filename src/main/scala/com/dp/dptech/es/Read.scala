package com.dp.dptech.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._

object Read {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("elasticSearch")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val rdd = sc.esRDD("radio/artists") 
    
    rdd.foreach(println)
    
    //Read data with a query parameter
    //create an RDD streaming all the documents matching me* from index radio/artists
    val rdd_query = sc.esRDD("radio/artists", "?q=me*") 
    rdd_query.foreach(println)
    
    //Reading data in JSON format
    val rdd_json = sc.esJsonRDD("radio/artists", "?q=me*") 
    rdd_query.foreach(println)
  }
}

