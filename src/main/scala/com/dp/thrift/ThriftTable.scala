package com.dp.thrift

/**
 * @author miraj
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Record(key: Int, value: String)

object ThriftTable {
  val jsonFile = "/mnt/git/spark/data/people.json"
  // set up the spark using SparkSession. No need to create SparkContext
  // You automatically get as part of the SparkSession
  // SparkSession uses the builder design to construct a SparkSession

  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("SparkSessionZipsExample_16")
    .set("spark.sql.warehouse.dir", warehouseLocation)
    .set("spark.files.overwrite", "true")

  val spark = new SparkContext(sparkConf)
  // create SQLContext
  val sqlContext = new SQLContext(spark)
  //read the json file, infere the schema and create a DataFrame
   val peopleDF = sqlContext.read.json(jsonFile)

  //show the tables's first 20 rows
  peopleDF.show(10)

  //display the the total number of rows in the dataframe
  println("Total number of zipcodes: " + peopleDF.count())

  //filter all persons with age greater than 10
  peopleDF.filter(peopleDF.col("age") > 10).show(10)

  // let's cache
  peopleDF.cache()

  //Now create an SQL table and issue SQL queries against it without
  // explicting using SQLContext but from SparkSession
  // Creates a temporary view using the DataFrame
  peopleDF.registerTempTable("people")

}