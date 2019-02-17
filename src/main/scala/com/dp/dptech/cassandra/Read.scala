package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 */
object Read {
def main(args: Array[String]) {
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
  //    .set("spark.cassandra.auth.username", "cassandra")
  //    .set("spark.cassandra.auth.password", "cassandra")

  val sc = new SparkContext(conf)

  val rdd = sc.cassandraTable("test", "words")
  rdd.foreach(println)

  val firstRow = rdd.first

  //Get the number of columns and column names:
  firstRow.nameOf(1) // Column name of ith index 
  firstRow.size // 2 

  //Use one of getXXX getters to obtain a column value converted to desired type:
  firstRow.getInt("count") // 20       
  firstRow.getLong("count") // 20L  

  //Or use a generic get to query the table by passing the return type directly:
  firstRow.get[Int]("count") // 20       
  firstRow.get[Long]("count") // 20L
  firstRow.get[BigInt]("count") // BigInt(20)
  firstRow.get[java.math.BigInteger]("count") // BigInteger(20)

  //When reading potentially null data, use the Option type on the Scala side to prevent getting a NullPointerException
  firstRow.getIntOption("count") // Some(20)
  firstRow.get[Option[Int]]("count") // Some(20)   

}
}


/**
Cassandra table scripts
CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE test.words (word text PRIMARY KEY, count int);

INSERT INTO test.words (word, count) VALUES ('foo', 20);
INSERT INTO test.words (word, count) VALUES ('bar', 20);
*/