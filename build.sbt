name := "spark impressions count"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

val sparkCassandraConnectorVersion = "2.0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion