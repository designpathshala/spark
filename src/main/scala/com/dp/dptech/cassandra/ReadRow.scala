package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 */
object ReadRow {
def main(args: Array[String]) {
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
  //    .set("spark.cassandra.auth.username", "cassandra")
  //    .set("spark.cassandra.auth.password", "cassandra")

  val sc = new SparkContext(conf)

  val rdd = sc.cassandraTable("test", "words")
  rdd.foreach(println)
}
}


/**
Cassandra table scripts
CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE test.words (word text PRIMARY KEY, count int);

INSERT INTO test.words (word, count) VALUES ('foo', 20);
INSERT INTO test.words (word, count) VALUES ('bar', 20);
*/