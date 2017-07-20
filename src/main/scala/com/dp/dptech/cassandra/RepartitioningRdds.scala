package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 * The method repartitionByCassandraReplica can be used to relocate data
 * in an RDD to match the replication strategy of a given table and keyspace.
 * The method will look for partition key information in the given RDD and then
 * use those values to determine which nodes in the Cluster would be responsible for
 * that data. You can control the resultant number of partitions with the parameter partitionsPerHost.
 */
object RepartitioningRdds {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
    //    .set("spark.cassandra.auth.username", "cassandra")
    //    .set("spark.cassandra.auth.password", "cassandra")

    val sc = new SparkContext(conf)

    val idsOfInterest = sc.parallelize(1 to 1000).map(CustomerID(_))
    val repartitioned = idsOfInterest.repartitionByCassandraReplica("test", "shopping_history", 10)
    repartitioned.partitions
    //res0: Array[org.apache.spark.Partition] = Array(ReplicaPartition(0,Set(/127.0.0.1)), ...)
    repartitioned.partitioner
    //res1: Option[org.apache.spark.Partitioner] = Some(com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner@4484d6c2)
    //scala> repartitioned
    //res2: com.datastax.spark.connector.rdd.partitioner.CassandraPartitionedRDD[CustomerID] = CassandraPartitionedRDD[5] at RDD at CassandraPartitionedRDD.scala:12
  }
}

case class CustomerID(cust_id: Int) 

/**
Cassandra table scripts
use test;
CREATE TABLE test.shopping_history ( cust_id INT, date TIMESTAMP,  product TEXT, quantity INT, PRIMARY KEY (cust_id, date, product));
INSERT INTO shopping_history(cust_id,date,product,quantity)
VALUES (12,'2013-04-03 07:01:00','72F',4);

INSERT INTO shopping_history(cust_id,date,product,quantity)
VALUES (14,'2013-04-03 07:02:00','73F',10);

INSERT INTO shopping_history(cust_id,date,product,quantity)
VALUES (19,'2013-04-03 07:03:00','73F',3);

INSERT INTO shopping_history(cust_id,date,product,quantity)
VALUES (30,'2013-04-03 07:04:00','74F',5);




 */

//Next -> JoinTwoTables
