package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 * The connector supports using any RDD as a source of a direct join with a
 * Cassandra Table through joinWithCassandraTable.
 *
 * Any RDD which is writable to a Cassandra table via the saveToCassandra method
 * can be used with this procedure as long as the full partition key is specified.
 * joinWithCassandraTable utilizes the java drive to execute a single query for
 * every partition required by the source RDD so no un-needed data will be requested or serialized.
 *
 * This means a join between any RDD and a Cassandra Table can be performed
 * without doing a full table scan. When performed between two Cassandra Tables which share
 * the same partition key this will not require movement of data between machines.
 * In all cases this method will use the source RDD's partitioning and placement for data locality.
 */
object JoinTwoTables {

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
    //(CassandraRow{cust_id: 30, address: Japan, name: Shrela},CassandraRow{cust_id: 30, date: 2013-04-03 07:04:00+0000, product: 74F, quantity: 5})
    //(CassandraRow{cust_id: 19, address: Bangalore, name: Krish},CassandraRow{cust_id: 19, date: 2013-04-03 07:03:00+0000, product: 73F, quantity: 3})
    //(CassandraRow{cust_id: 14, address: Jaipur, name: Jasmine},CassandraRow{cust_id: 14, date: 2013-04-03 07:02:00+0000, product: 73F, quantity: 10})
    //(CassandraRow{cust_id: 12, address: Texas, name: rita},CassandraRow{cust_id: 12, date: 2013-04-03 07:01:00+0000, product: 72F, quantity: 4})

    //Example Join with Generic RDD
    /**
     * The repartitionByCassandraReplica method can be used prior to calling joinWithCassandraTable
     * to obtain data locality, such that each spark partition will only require queries to their local node.
     * This method can also be used with two Cassandra Tables which have partitioned with different partition keys.
     */
    val joinWithRDD = sc.parallelize(0 to 50).filter(_ % 2 == 0).map(CustomerID(_)).joinWithCassandraTable("test", "customer_info")
    joinWithRDD.collect.foreach(println)
    //(CustomerID(12),CassandraRow{cust_id: 12, address: Texas, name: rita})
    //(CustomerID(14),CassandraRow{cust_id: 14, address: Jaipur, name: Jasmine})
    //(CustomerID(30),CassandraRow{cust_id: 30, address: Japan, name: Shrela})

  }
}

//case class WordCount(word: String, count: Int)

/**
Cassandra table scripts
use test;
CREATE TABLE test.customer_info ( cust_id INT, name TEXT, address TEXT, PRIMARY KEY (cust_id));

INSERT INTO customer_info(cust_id,name,address)
VALUES (12,'rita','Texas');

INSERT INTO customer_info(cust_id,name,address)
VALUES (14,'Jasmine','Jaipur');

INSERT INTO customer_info(cust_id,name,address)
VALUES (19,'Krish','Bangalore');

INSERT INTO customer_info(cust_id,name,address)
VALUES (30,'Shrela','Japan');
 */

//Previous -> RepartitioningRdds
