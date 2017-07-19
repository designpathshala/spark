package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 */
object ReadCollections {
  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    val row = sc.cassandraTable("test", "users").first

    println(row.getList[String]("emails")) // Vector(someone@email.com, s@email.com)
    println(row.get[List[String]]("emails")) // List(someone@email.com, s@email.com)    
    println(row.get[Seq[String]]("emails")) // List(someone@email.com, s@email.com)   :Seq[String]
    println(row.get[IndexedSeq[String]]("emails")) // Vector(someone@email.com, s@email.com) :IndexedSeq[String]
    println(row.get[Set[String]]("emails")) // Set(someone@email.com, s@email.com)

    //It is also possible to convert a collection to CQL String representation:
    println(row.get[String]("emails")) // "[someone@email.com, s@email.com]"
  }
}


/**
Cassandra table scripts
CREATE TABLE test.users (username text PRIMARY KEY, emails SET<text>);
INSERT INTO test.users (username, emails) 
     VALUES ('someone', {'someone@email.com', 's@email.com'});
*/