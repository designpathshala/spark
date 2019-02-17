package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 */
object JoinsAdvanced {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
    //    .set("spark.cassandra.auth.username", "cassandra")
    //    .set("spark.cassandra.auth.password", "cassandra")

    val sc = new SparkContext(conf)

    val internalJoin = sc.cassandraTable("test","customer_info").joinWithCassandraTable("test","shopping_history")
    val recentOrders = internalJoin.where("date > '2015-03-09'") // Where applied to every partition
    val someOrders = internalJoin.limit(1) // Returns at most 1 CQL Row per Spark Partition
    val numOrders = internalJoin.count() // Sums the total number of cql Rows
    val orderQuantities = internalJoin.select("quantity") // Returns only the amount column as the right side of the join
    val specifiedJoin = internalJoin.on(SomeColumns("cust_id")) // Joins on the cust_id column
    val emptyJoin = internalJoin.toEmptyCassandraRDD // Makes an EmptyRDD

  }
}

/**
Cassandra table scripts
 */
