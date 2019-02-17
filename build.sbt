import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._
EclipseKeys.withSource := true

name := "dp-spark"

version := "0.1.0"

scalaVersion := "2.10.4"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// additional libraries
libraryDependencies ++= Seq(
  // spark 1.5.0
  // "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
  // "org.apache.spark" %% "spark-hive" % "1.5.0",
  // "org.apache.spark" %% "spark-sql" % "1.5.0",
  // "org.apache.spark" %% "spark-streaming" % "1.5.0",
  // "org.apache.spark" %% "spark-streaming-kafka" % "1.5.0",
  //  "org.apache.spark" %% "spark-streaming-twitter" % "1.5.0",
  //  "org.apache.spark" %% "spark-graphx" % "1.5.0",
  //  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0"
   
   // spark 1.6.0
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-streaming" % "1.6.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0" ,
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0",
     "org.apache.spark" %% "spark-graphx" % "1.6.0",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.3"
  

  // spark 2.1.0
 // "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
 // "org.apache.spark" %% "spark-hive" % "2.1.0",
 // "org.apache.spark" %% "spark-sql" % "2.1.0",
 // "org.apache.spark" %% "spark-streaming" % "2.1.0",
 // "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"  
 // "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0",
 // "org.apache.spark" % "spark-streaming-twitter" % "1.6.0"
  )

resolvers ++= Seq(
  "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  Resolver.sonatypeRepo("public")
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  //case m if m.contains("jackson")        	   => MergeStrategy.discard
  //case m if m.contains("com.fasterxml.jackson")        	   => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
