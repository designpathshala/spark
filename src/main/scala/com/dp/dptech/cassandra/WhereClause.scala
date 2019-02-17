package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 */
object WhereClause {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
    //    .set("spark.cassandra.auth.username", "cassandra")
    //    .set("spark.cassandra.auth.password", "cassandra")

    val sc = new SparkContext(conf)

//    sc.cassandraTable("test", "cars").select("id", "model").where("color = ?", "black").toArray.foreach(println)
//    // CassandraRow[id: KF-334L, model: Ford Mondeo]
//    // CassandraRow[id: MT-8787, model: Hyundai x35]
//
//    sc.cassandraTable("test", "cars").select("id", "model").where("color = ?", "silver").toArray.foreach(println)
    // CassandraRow[id: WX-2234, model: Toyota Yaris] 

    //Grouping rows by partition key
    sc.cassandraTable("test", "events")
      .spanBy(row => (row.getInt("year"), row.getInt("month")))

    sc.cassandraTable("test", "events")
      .keyBy(row => (row.getInt("year"), row.getInt("month")))
      .spanByKey

    //Mapping rows to (case) objects
    val result = sc.cassandraTable[WordCount]("test", "words").select("word", "num" as "count").collect()
  }
}

case class WordCount(word: String, count: Int)

/**
Cassandra table scripts
CREATE TABLE events (year int, month int, ts timestamp, data varchar, PRIMARY KEY (year,month,ts));
INSERT INTO test.words (word, count) VALUES ('foo', 20);
INSERT INTO test.words (word, count) VALUES ('bar', 20);
*/