package com.dp.dptech.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

/**
 * @author miraj
 */
class ReadUDT {
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
  //    .set("spark.cassandra.auth.username", "cassandra")
  //    .set("spark.cassandra.auth.password", "cassandra")

  val sc = new SparkContext(conf)

  val rdd = sc.cassandraTable("test", "companies")
  val row = rdd.first

  val address: UDTValue = row.getUDTValue("address")
  val city = address.getString("city")
  val street = address.getString("street")
  val number = address.getInt("number")

}


/**
Cassandra table scripts
CREATE TYPE test.address (city text, street text, number int);
CREATE TABLE test.companies (name text PRIMARY KEY, address FROZEN<address>);
INSERT INTO test.companies (name, address) VALUES (
  'FRAME', 
  { city : 'NY', street : 'Richmond Street', number : '908089' }
);
*/
