import AssemblyKeys._
EclipseKeys.withSource := true

name := "dp-spark"

version := "0.1.0"

scalaVersion := "2.10.4"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// additional libraries
libraryDependencies ++= Seq(
  // spark 1.5.0
  "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.5.0",
  "org.apache.spark" %% "spark-sql" % "1.5.0",
  "org.apache.spark" %% "spark-streaming" % "1.5.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.5.0"  
  )

resolvers ++= Seq(
  "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  Resolver.sonatypeRepo("public")
)

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  //case m if m.contains("jackson")        	   => MergeStrategy.discard
  //case m if m.contains("com.fasterxml.jackson")        	   => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
