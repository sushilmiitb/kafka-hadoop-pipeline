name := "aggregator"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

// Remove provided to run locally
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "com.chymeravr.schemas" % "schemas" % "0.1-SNAPSHOT"
libraryDependencies += "com.chymeravr.pipeline" % "eventjoin" % "1.0-SNAPSHOT"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}