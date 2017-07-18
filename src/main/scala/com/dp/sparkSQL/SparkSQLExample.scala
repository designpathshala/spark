package com.dp.sparkSQL

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql.SQLContext

object SparkSQLExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Spark SQL basic example")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // For implicit conversions like converting RDDs to DataFrames
    runBasicDataFrameExample(sqlContext)
    runInferSchemaExample(sqlContext)
    runProgrammaticSchemaExample(sqlContext)

    sc.stop()
  }

  private def runBasicDataFrameExample(sqlContext: SQLContext): Unit = {
    // $example on:create_df$
    val df = sqlContext.read.json("/user/hue/dp/spark/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_df$

    import sqlContext.implicits._
    // Print the schema in a tree format
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+

    // Register the DataFrame as a SQL temporary view
    df.registerTempTable("people")

    val sqlDF = sqlContext.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
  }


  /**
   * User power of spark to infer schema
   */
  private def runInferSchemaExample(sqlContext: SQLContext): Unit = {
    // $example on:schema_inferring$
    // For implicit conversions from RDDs to DataFrames
    import sqlContext.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = sqlContext.sparkContext
      .textFile("/user/hue/dp/spark/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = sqlContext.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    val teenagerMapped = teenagersDF.map(teenager => "Name: " + teenager(0))
    teenagerMapped.foreach { x => println(x) }
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // or by field name
     val teenagerMapped1 = teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name"))
     teenagerMapped1.foreach { x => println(x) }
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

  }

  /**
   * Create schema programatically
   * using StructType and Row
   */
  private def runProgrammaticSchemaExample(sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    // $example on:programmatic_schema$
    // Create an RDD
    val peopleRDD = sqlContext.sparkContext.textFile("/user/hue/dp/spark/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = sqlContext.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.registerTempTable("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = sqlContext.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).first()
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
  }
}

// /mnt/spark/bin/spark-submit --class sparkSQL.SparkSQLExample --jars /mnt/git/spark/target/scala-2.10/adtech-assembly-0.1.0-deps.jar /mnt/git/spark/target/scala-2.10/adtech_2.10-0.1.0.jar