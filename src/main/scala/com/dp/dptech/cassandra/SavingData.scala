package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 */
object SavingData {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
    //    .set("spark.cassandra.auth.username", "cassandra")
    //    .set("spark.cassandra.auth.password", "cassandra")

    val sc = new SparkContext(conf)

    val collection = sc.parallelize(Seq(("cat", 30), ("fox", 40)))
    collection.saveToCassandra("test", "words", SomeColumns("word", "count"))

    //Example Saving an RDD of Tuples with Custom Mapping
    val collection1 = sc.parallelize(Seq((30, "cat"), (40, "fox")))
    collection1.saveToCassandra("test", "words", SomeColumns("word" as "_2", "count" as "_1"))

    //Example Saving an RDD of Scala Objects
    val collection2 = sc.parallelize(Seq(WordCount("dog", 50), WordCount("cow", 60)))
    collection2.saveToCassandra("test", "words", SomeColumns("word", "count"))

    //Example Saving an RDD of Scala Objects with Custom Mapping
    val collection3 = sc.parallelize(Seq(WordCount("dog", 50), WordCount("cow", 60)))
    collection3.saveToCassandra("test", "words2", SomeColumns("word", "num" as "count"))
  }
}
//case class WordCount(word: String, count: Long)


/**
Cassandra table scripts
CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE test.words (word text PRIMARY KEY, count int);
CREATE TABLE test.words2 (word text PRIMARY KEY, num int);
*/

//Previous JoinsAdvanced
