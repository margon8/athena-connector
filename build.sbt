name := "athena-connector"

version := "0.1"

scalaVersion := "2.13.5"

val awsVersion = "1.2.1"

libraryDependencies += "com.amazonaws" % "aws-lambda-java-core" % "1.2.1" % Provided
libraryDependencies += "com.amazonaws" % "aws-lambda-java-events" % "2.2.9" % Provided
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6"

libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0"
//libraryDependencies += "com.amazonaws" % "aws-lambda-java-log4j2" % "1.1.0"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % "test"

//Added as local dependency
//libraryDependencies += "com.amazonaws" % "aws-athena-federation-sdk" % "2021.14.1"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}