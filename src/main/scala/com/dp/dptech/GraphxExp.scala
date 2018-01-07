package adhocWork

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.spark.sql.Row

/**
 * @author miraj
 */
class GraphxExp {
  // function to parse input into Flight class
  def parseFlight(row: Row): Flight = {
    Flight(row.getAs[Int]("Month"), row.getAs[Int]("DayOfWeek"), row.getAs("Carrier"), row.getAs("TailNum"), row.getAs[Integer]("FlightNum"), row.getAs[Int]("OriginAirportID"), row.getAs("Origin"), row.getAs[Int]("DestAirportID"), row.getAs[String]("Dest"), row.getAs[Int]("CRSDepTime"), row.getAs[Int]("DepTime"), row.getAs[Double]("DepDelayMinutes"), row.getAs[Int]("CRSArrTime"), row.getAs[Int]("ArrTime"), row.getAs[Double]("ArrDelay"), row.getAs[Double]("CRSElapsedTime"), row.getAs[Double]("Distance"))
  }
  def main(args: Array[String]) {
    val conf = new SparkConf(true)

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true")
      .load("/tmp/miraj/graphx")

    // parse the RDD of csv lines into an RDD of flight classes</font>
    val flightsRDD = df.map(parseFlight).cache()
    flightsRDD.first

    // create airports RDD with ID and Name

    val airports = flightsRDD.map(flight => (flight.org_id.toLong, flight.origin)).distinct

    // Defining a default vertex called nowhere</font>
    val nowhere = "nowhere"

    // Map airport ID to the 3-letter code to use for printlns</font>
    val airportMap = airports.map { case ((org_id), name) => (org_id -> name) }.collect.toList.toMap

    // create routes RDD with srcid, destid, distance</font>
    val routes = flightsRDD.map(flight => ((flight.org_id, flight.dest_id), flight.dist)).distinct

    // create edges RDD with srcid, destid , distance</font>
    val edges = routes.map {
      case ((org_id, dest_id), distance) => Edge(org_id.toLong, dest_id.toLong, distance)
    }

    // define the graph
    val graph = Graph(airports, edges, nowhere)

    // graph vertices
    graph.vertices.take(2)

    // graph edges
    graph.edges.take(2)

    // How many airports?
    val numairports = graph.numVertices

    // How many routes?
    val numroutes = graph.numEdges

    // routes > 1000 miles distance?
    graph.edges.filter { case (Edge(org_id, dest_id, distance)) => distance > 1000 }.take(3)

    //EdgeTriplet class
    graph.triplets.take(3).foreach(println)

    // print out longest routes
    graph.triplets.sortBy(_.attr, ascending = false).map(triplet =>
      "Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10).foreach(println)

    // Define a reduce operation to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int) = graph.degrees.reduce(max)

    // Get the name for the airport with id 10397
    airportMap(10397)

    //Which airport has the most incoming flights?
    val maxIncoming = graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (airportMap(x._1.toInt), x._2)).take(3)

    maxIncoming.foreach(println)

    //// which airport has the most outgoing flights?
    val maxout = graph.outDegrees.join(airports).sortBy(_._2._1, ascending = false).take(3)

    maxout.foreach(println)

    //What are the most important airports according to PageRank?
    // use pageRank
    val ranks = graph.pageRank(0.1).vertices

    // join the ranks  with the map of airport id to name
    val temp = ranks.join(airports)
    temp.take(1).foreach(println)

    // sort by ranking
    val temp2 = temp.sortBy(_._2._1, false)
    temp2.take(2).foreach(println)

    // get just the airport names
    val impAirports = temp2.map(_._2._2)
    impAirports.take(4).foreach(println)
  }

}
case class Flight(dofM: Int, dofW: Int, carrier: String, tailnum: String, flnum: Int, org_id: Int, origin: String, dest_id: Int, dest: String, crsdeptime: Int, deptime: Int, depdelaymins: Double, crsarrtime: Int, arrtime: Int, arrdelay: Double, crselapsedtime: Double, dist: Double)

//spark-shell --master yarn-client --num-executors 5 --queue spark --packages com.databricks:spark-csv_2.10:1.5.0
