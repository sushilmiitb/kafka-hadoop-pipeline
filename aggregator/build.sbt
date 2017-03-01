name := "aggregator"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

// Remove provided to run locally
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "com.chymeravr.schemas" % "schemas" % "0.1-SNAPSHOT"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"
libraryDependencies += "postgresql" % "postgresql" % "9.4.1208-jdbc42-atlassian-hosted"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}