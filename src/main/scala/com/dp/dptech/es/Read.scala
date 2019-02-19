package com.dp.dptech.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._
import java.util.Properties

object Read {

  val props = new Properties()

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("elasticSearch")
    conf.set("es.index.auto.create", "true")

    if (args.length < 1) {
      System.err.println("Usage: DP dptech parameters <mode> ")
      System.exit(1)
    }
    val Array(jobMode) = args

    // load a properties file
    props.load(getClass().getClassLoader().getResourceAsStream(jobMode + "/spark-job.properties"));

    val es_nodes = props.getProperty("es.nodes")
    val es_port = props.getProperty("es.port")
    conf.set("es.nodes", es_nodes)
    conf.set("es.port", es_port)
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

