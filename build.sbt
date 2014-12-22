name := "kalin"

version := "0.1.0"

scalaVersion := "2.10.4"

conflictManager := ConflictManager.latestRevision

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.1" % "provided"

libraryDependencies += "joda-time" % "joda-time" % "2.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.0" % "test"