package com.dp.dptech.es

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._
import java.util.Properties

object Write {
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

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    //another way to pass es conf while saving data
    //println("nodes -------------"+ es_nodes)
    //println("port---------------" + es_port)
    //var esconf = Map("es.nodes" -> es_nodes, "es.port" -> es_port)
    //sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs",esconf)
  }
}

