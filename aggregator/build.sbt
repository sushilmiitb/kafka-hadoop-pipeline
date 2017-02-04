name := "aggregator"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "com.chymeravr.schemas" % "schemas" % "0.1-SNAPSHOT"
libraryDependencies += "com.chymeravr.pipeline" % "eventjoin" % "1.0-SNAPSHOT"
