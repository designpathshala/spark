package com.dp.dptech.cassandra
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

import org.apache.spark.SparkContext

/**
 * @author miraj
 */
class ClusterDataTransfer {

  def twoClusterExample(sc: SparkContext) = {
    val connectorToClusterOne = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", "127.0.0.1"))
    val connectorToClusterTwo = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", "127.0.0.2"))

    val rddFromClusterOne = {
      // Sets connectorToClusterOne as default connection for everything in this code block
      implicit val c = connectorToClusterOne
      sc.cassandraTable("ks", "tab")
    }

    {
      //Sets connectorToClusterTwo as the default connection for everything in this code block
      implicit val c = connectorToClusterTwo
      rddFromClusterOne.saveToCassandra("ks", "tab")
    }

  }
}