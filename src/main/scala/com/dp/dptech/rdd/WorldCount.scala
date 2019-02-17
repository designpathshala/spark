package com.dp.dptech.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author miraj
 */
object WorldCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("rddBasics")
    val sc = new SparkContext(conf)

    //Creating RDD of strings with textFile
    val inputRDD = sc.textFile("/mnt/git/spark/data/mysqld.log")

    // Word count – traditional map-reduce.  Prints count of words in error lines
    val wordCountsErrors = inputRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCountsErrors.foreach(x => println(x))
   
  }
}

// spark-submit --class com.dp.dptech.rdd.WorldCount --jars /usr/lib/hue/designpathshala/spark/dp-spark-assembly-0.1.0-deps.jar /usr/lib/hue/designpathshala/spark/dp-spark_2.10-0.1.0.jar