package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 */
object Write {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
    //    .set("spark.cassandra.auth.username", "cassandra")
    //    .set("spark.cassandra.auth.password", "cassandra")

    val sc = new SparkContext(conf)

    val internalJoin = sc.cassandraTable("test", "customer_info").joinWithCassandraTable("test", "shopping_history")
    internalJoin.toDebugString
    //res4: String = (1) CassandraJoinRDD[9] at RDD at CassandraRDD.scala:14 []
    internalJoin.collect
    internalJoin.collect.foreach(println)
  }
}


/**
Cassandra table scripts
CREATE TABLE test.customer_info ( cust_id INT, name TEXT, address TEXT, PRIMARY KEY (cust_id));
CREATE TABLE test.shopping_history ( cust_id INT, date TIMESTAMP,  product TEXT, quantity INT, PRIMARY KEY (cust_id, date, product));
*/
