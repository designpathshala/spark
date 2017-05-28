package com.dp.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author miraj
 */
object SparkBasics {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("rddBasics")
    val sc = new SparkContext(conf)

    //Creating RDD of strings with textFile
    val inputRDD = sc.textFile("/mnt/git/spark/data/mysqld.log")

    //print top ten lines of rdd
    inputRDD.take(10).foreach(println)

    //filter the rdd, its case sensitive
    var errorsRDD = inputRDD.filter(x => x.contains("ERROR"))
    //Prints numver of lines with error 
    errorsRDD.count

    //Prints all the lines with error
    errorsRDD.foreach(x => println(x))

    // Prints length of line with most words in errors.  Reduce is an action.
    errorsRDD.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)

    // Word count – traditional map-reduce.  Prints count of words in error lines
    val wordCountsErrors = errorsRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCountsErrors.foreach(x => println(x))
    
    //Print only words appearing more than 5 times in error texts
    wordCountsErrors.filter(x => x._2 > 5).foreach(x => println(x))

    //Calcualte avg using aggregrate function
    val z = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    val result = z.aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avg = result._1 / result._2.toDouble
  }
}